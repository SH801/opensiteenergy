import os
import subprocess
import json
import logging
import time
from pathlib import Path
from psycopg2 import sql, Error
from opensite.constants import OpenSiteConstants
from opensite.processing.base import ProcessBase
from opensite.logging.opensite import OpenSiteLogger
from opensite.postgis.opensite import OpenSitePostGIS
from opensite.model.graph.opensite import OpenSiteGraph

PROCESSINGGRID_SQUARE_IDS = None

class OpenSiteSpatial(ProcessBase):

    PROCESSING_INTERVAL_TIME = 5

    def __init__(self, node, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteSpatial", log_level, shared_lock)
        self.base_path = OpenSiteConstants.DOWNLOAD_FOLDER
        self.postgis = OpenSitePostGIS(log_level)
        
    def get_crs_number(self):
        """
        Get default CRS as number - for use in PostGIS
        """

        return OpenSiteConstants.CRS_DEFAULT.replace('EPSG:', '')
    
    def import_clipping_master(self):
        """
        Imports clipping master if not already imported
        """

        clipping_master_file = OpenSiteConstants.CLIPPING_MASTER
        clipping_master_table = OpenSiteConstants.OPENSITE_CLIPPINGMASTER

        if self.postgis.table_exists(clipping_master_table): return True

        return self.postgis.import_spatial_data(clipping_master_file, clipping_master_table)

    def create_processing_grid(self):
        """
        Creates processing grid
        Due to issues with calling this within parallel processor setting
        this should be called during main application initialization
        """

        global PROCESSINGGRID_SQUARE_IDS

        if not self.import_clipping_master():
            self.log.error(f"Problem importing clipping master")
            return False

        if self.postgis.table_exists(OpenSiteConstants.OPENSITE_PROCESSINGGRID):
            self.log.info("Processing grid already exists")
            self.get_processing_grid_square_ids()
            return True

        self.log.info(f"Creating grid overlay with grid size {OpenSiteConstants.PROCESSINGGRID_SPACING} to reduce memory load during ST_Union")

        dbparams = {
            "crs": sql.Literal(self.get_crs_number()),
            "grid": sql.Identifier(OpenSiteConstants.OPENSITE_PROCESSINGGRID),
            "grid_index": sql.Identifier(f"{OpenSiteConstants.OPENSITE_PROCESSINGGRID}_idx"),
            "grid_spacing": sql.Literal(OpenSiteConstants.PROCESSINGGRID_SPACING),
            "clipping_master": sql.Identifier(OpenSiteConstants.OPENSITE_CLIPPINGMASTER)
        }

        query_grid_create = sql.SQL("""
        CREATE TABLE {grid} AS 
        SELECT 
            (ST_SquareGrid({grid_spacing}, ST_SetSRID(extent_geom, {crs}))).geom::geometry(Polygon, {crs}) as geom
        FROM (
            SELECT ST_Extent(geom)::geometry as extent_geom 
            FROM {clipping_master}
        ) AS sub;
        """).format(**dbparams)
        query_grid_alter = sql.SQL("ALTER TABLE {grid} ADD COLUMN id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY").format(**dbparams)
        query_grid_create_index = sql.SQL("CREATE INDEX {grid_index} ON {grid} USING GIST (geom)").format(**dbparams)
        query_grid_delete_squares = sql.SQL("DELETE FROM {grid} g WHERE NOT EXISTS (SELECT 1 FROM {clipping_master} c WHERE ST_Intersects(g.geom, c.geom))").format(**dbparams)

        try:
            self.postgis.execute_query(query_grid_create)
            self.postgis.execute_query(query_grid_alter)
            self.postgis.execute_query(query_grid_delete_squares)
            self.postgis.execute_query(query_grid_create_index)
            self.get_processing_grid_square_ids()

            self.log.info(f"Finished creating grid overlay with grid size {OpenSiteConstants.PROCESSINGGRID_SPACING}")

            return True
        except Error as e:
            self.log.error(f"PostGIS Error during grid creation: {e}")
            return False
        except Exception as e:
            self.log.error(f"Unexpected error: {e}")
            return False

    def get_processing_grid_square_ids(self):
        """
        Gets ids of all squares in processing grid
        """

        global PROCESSINGGRID_SQUARE_IDS

        if not PROCESSINGGRID_SQUARE_IDS:
            if not self.postgis.table_exists(OpenSiteConstants.OPENSITE_PROCESSINGGRID):
                self.log.error("Processing grid does not exist, unable to retrieve grid square ids")
                return None
            
            results = self.postgis.fetch_all(sql.SQL("SELECT id FROM {grid}").format(grid=sql.Identifier(OpenSiteConstants.OPENSITE_PROCESSINGGRID)))
            PROCESSINGGRID_SQUARE_IDS = [row['id'] for row in results]

        return PROCESSINGGRID_SQUARE_IDS

    def buffer(self):
        """
        Adds buffer to spatial dataset 
        Buffering is always added before dataset is split into grid squares
        """
            
        if self.postgis.table_exists(self.node.output):
            self.log.info(f"[{self.node.output}] already exists, skipping buffer for {self.node.name}")
            self.node.status = 'processed'
            return True

        if 'buffer' not in self.node.custom_properties:
            self.log.error(f"{self.node.name} is missing 'buffer' field, buffering failed")
            self.node.status = 'failed'
            return False
         
        buffer = self.node.custom_properties['buffer']
        input_table = self.node.input
        output_table = self.node.output

        self.log.info(f"[{self.node.name}] Adding {buffer}m buffer to {input_table} to make {output_table}")

        dbparams = {
            "input": sql.Identifier(input_table),
            "output": sql.Identifier(output_table),
            "output_index": sql.Identifier(f"{output_table}_idx"),
            "buffer": sql.Literal(buffer),
        }

        query_buffer_create = sql.SQL("CREATE TABLE {output} AS SELECT ST_Buffer(geom, {buffer}) geom FROM {input}").format(**dbparams)
        query_buffer_create_index = sql.SQL("CREATE INDEX {output_index} ON {output} USING GIST (geom)").format(**dbparams)

        # Make special exception for hedgerow as hedgerow polygons represent boundaries that should be buffered as lines
        buffer_polygons_as_lines = False
        if 'hedgerows--' in self.node.name: buffer_polygons_as_lines = True

        if buffer_polygons_as_lines:
            query_buffer_create = sql.SQL("""
            CREATE TABLE {output} AS 
            (
                (SELECT ST_Buffer(geom, {buffer}) geom FROM {input} WHERE ST_geometrytype(geom) = 'ST_LineString') UNION 
                (SELECT ST_Buffer(ST_Boundary(geom), {buffer}) geom FROM {input} WHERE ST_geometrytype(geom) IN ('ST_Polygon', 'ST_MultiPolygon'))
            )
            """).format(**dbparams)

        try:
            self.postgis.execute_query(query_buffer_create)
            self.postgis.execute_query(query_buffer_create_index)
            self.postgis.add_table_comment(self.node.output, self.node.name)

            # Success Gate: Only update registry now
            if self.postgis.set_table_completed(self.node.output):
                self.log.info(f"[{self.node.name}] Finished adding {buffer}m buffer to {input_table} to make {output_table}")
                return True
            else:
                # This catches the bug where the node was never registered initially
                self.log.error(f"Buffer added but registry record for {self.node.output} was not found.")
                return False

        except Error as e:
            self.log.error(f"[{self.node.name}] PostGIS error during buffer creation: {e}")
            return False
        except Exception as e:
            self.log.error(f"[{self.node.name}] Unexpected error: {e}")
            return False

    def preprocess(self):
        """
        Preprocess node - dump to produce single geometry type then split into grid squares
        """

        if self.postgis.table_exists(self.node.output):
            self.log.info(f"[{self.node.output}] already exists, skipping preprocess for {self.node.name}")
            self.node.status = 'processed'
            return True
    
        if not self.postgis.table_exists(OpenSiteConstants.OPENSITE_PROCESSINGGRID):
            self.log.info("Processing grid does not exist, creating it...")
            if not self.create_processing_grid():
                self.log.error(f"Failed to create processing grid, unable to preprocess {self.node.name}")
                self.node.status = 'failed'
                return False
            
        grid_table = OpenSiteConstants.OPENSITE_PROCESSINGGRID
        gridsquare_ids = self.get_processing_grid_square_ids()
        scratch_table_1 = '_s1_' + self.node.output
        scratch_table_2 = '_s2_' + self.node.output

        dbparams = {
            "crs": sql.Literal(self.get_crs_number()),
            "grid": sql.Identifier(grid_table),
            "input": sql.Identifier(self.node.input),
            "scratch1": sql.Identifier(scratch_table_1),
            "scratch2": sql.Identifier(scratch_table_2),
            "output": sql.Identifier(self.node.output),
            "scratch1_index": sql.Identifier(f"{scratch_table_1}_idx"),
            "scratch2_index": sql.Identifier(f"{scratch_table_2}_idx"),
            "output_index": sql.Identifier(f"{self.node.output}_idx"),
        }

        # Drop scratch tables
        self.postgis.drop_table(scratch_table_1)
        self.postgis.drop_table(scratch_table_2)

        # Explode geometries with ST_Dump to remove MultiPolygon,
        # MultiSurface, etc and homogenize processing
        # Ideally all dumped tables should contain polygons only (either source or buffered source is (Multi)Polygon)
        # so filter on ST_Polygon

        query_s1_dump_makevalid = sql.SQL("""
        CREATE TABLE {scratch1} AS 
            SELECT  ST_MakeValid(dumped.geom) geom 
            FROM    (SELECT (ST_Dump(geom)).geom geom FROM {input}) dumped 
            WHERE   ST_geometrytype(dumped.geom) = 'ST_Polygon'
        """).format(**dbparams)
        query_s2_dump       = sql.SQL("CREATE TABLE {scratch2} AS SELECT (ST_Dump(geom)).geom geom FROM {scratch1}").format(**dbparams)
        query_output_create = sql.SQL("CREATE TABLE {output} (id INTEGER, geom GEOMETRY(Polygon, {crs}))").format(**dbparams)
        query_s1_index      = sql.SQL("CREATE INDEX {scratch1_index} ON {scratch1} USING GIST (geom)").format(**dbparams)
        query_s2_index      = sql.SQL("CREATE INDEX {scratch2_index} ON {scratch2} USING GIST (geom)").format(**dbparams)
        query_output_index  = sql.SQL("CREATE INDEX {output_index} ON {output} USING GIST (geom)").format(**dbparams)
        
        try:
            self.log.info(f"[{self.node.name}] Preprocess: Select only polygons, dump and make valid")

            self.postgis.execute_query(query_s1_dump_makevalid)
            self.postgis.execute_query(query_s1_index)

            self.log.info(f"[{self.node.name}] Dumping geometries")

            self.postgis.execute_query(query_s2_dump)
            self.postgis.execute_query(query_s2_index)

            self.log.info(f"[{self.node.name}] Creating preprocess table {self.node.output} by dissolving dataset")

            self.postgis.execute_query(query_output_create)
            self.postgis.add_table_comment(self.node.output, self.node.name)

            gridsquares_index, gridsquares_count = 0, len(gridsquare_ids)
            last_log_time = time.time()

            for gridsquare_id in gridsquare_ids:
                gridsquares_index += 1

                # Progress reporting - log every PROCESSING_INTERVAL_TIME seconds to avoid flooding terminal
                current_time = time.time()
                if  (gridsquares_index == 1) or \
                    (gridsquares_index == gridsquares_count) or \
                    (current_time - last_log_time > self.PROCESSING_INTERVAL_TIME):
                    self.log.info(f"[{self.node.name}] Processing grid square {gridsquares_index}/{gridsquares_count}")

                dbparams['gridsquare_id'] = sql.Literal(gridsquare_id)
                
                query_output_process_gridsquare = sql.SQL("""
                INSERT INTO {output} 
                    SELECT  grid.id, (ST_Dump(ST_Union(ST_Intersection(grid.geom, dataset.geom)))).geom geom 
                    FROM {grid} grid, {scratch2} dataset 
                    WHERE grid.id = {gridsquare_id} AND ST_geometrytype(dataset.geom) = 'ST_Polygon' GROUP BY grid.id""").format(**dbparams)
                self.postgis.execute_query(query_output_process_gridsquare)

            self.postgis.execute_query(query_output_index)

            self.postgis.drop_table(scratch_table_1)
            self.postgis.drop_table(scratch_table_2)

            # Success Gate: Only update registry now
            if self.postgis.set_table_completed(self.node.output):
                self.log.info(f"[{self.node.name}] Preprocess: COMPLETED")
                return True
            else:
                # This catches the bug where the node was never registered initially
                self.log.error(f"Preprocess completed but registry record for {self.node.output} was not found.")
                return False

            return True
        except Error as e:
            self.log.error(f"[{self.node.name}] PostGIS error during buffer creation: {e}")
            return False
        except Exception as e:
            self.log.error(f"[{self.node.name}] Unexpected error: {e}")
            return False

    def amalgamate(self):
        """
        Amalgamates datasets into one
        Note: amalgamate is universally applied to all geographical subcomponents even if one subcomponent
        """

        if self.postgis.table_exists(self.node.output):
            self.log.info(f"[{self.node.output}] already exists, skipping amalgamate")
            self.node.status = 'processed'
            return True

        if not self.postgis.table_exists(OpenSiteConstants.OPENSITE_PROCESSINGGRID):
            self.log.info("Processing grid does not exist, creating it...")
            if not self.create_processing_grid():
                self.log.error(f"Failed to create processing grid, unable to amalgamate {self.node.name}")
                self.node.status = 'failed'
                return False

        grid_table = OpenSiteConstants.OPENSITE_PROCESSINGGRID
        gridsquare_ids = self.get_processing_grid_square_ids()
        scratch_table_1 = '_s1_' + self.node.output
        children = self.node.custom_properties['children']

        dbparams = {
            "crs": sql.Literal(self.get_crs_number()),
            "grid": sql.Identifier(grid_table),
            "scratch1": sql.Identifier(scratch_table_1),
            "output": sql.Identifier(self.node.output),
            "scratch1_index": sql.Identifier(f"{scratch_table_1}_idx"),
            "output_index": sql.Identifier(f"{self.node.output}_idx"),
        }

        # Drop scratch tables
        self.postgis.drop_table(scratch_table_1)

        try:
            self.log.info(f"[{self.node.name}] Amalgamate: Starting amalgamation and dissolving")

            # Create output table regardless of number of children
            self.postgis.execute_query(sql.SQL("CREATE UNLOGGED TABLE {output} (id int, geom geometry(Geometry, {crs}))").format(**dbparams))
            self.postgis.add_table_comment(self.node.output, self.node.name)

            if len(children) == 1:
                dbparams['input'] = sql.Identifier(children[0])
                self.log.info(f"[{self.node.name}] Single child so directly copying from {dbparams['input']} to {dbparams['output']}")
                self.postgis.execute_query(sql.SQL("INSERT INTO {output} SELECT * FROM {input}").format(**dbparams))
                self.postgis.execute_query(sql.SQL("CREATE INDEX ON {output} USING GIST (geom)").format(**dbparams))
                return True

            # Create empty tables first using UNLOGGED for speed
            self.postgis.execute_query(sql.SQL("CREATE UNLOGGED TABLE {scratch1} (id int, geom geometry(Geometry, {crs}))").format(**dbparams))
    
            # Pour each child table in one by one
            child_index = 0
            for child in children:
                child_index += 1
                dbparams['input'] = sql.Identifier(child)
                self.log.info(f"[{self.node.name}] Amalgamating child table {child_index}/{len(children)}")
                query_add_table = sql.SQL("INSERT INTO {scratch1} (id, geom) SELECT id, (ST_Dump(geom)).geom FROM {input}").format(**dbparams)
                self.postgis.execute_query(query_add_table)

            self.postgis.execute_query(sql.SQL("CREATE INDEX ON {scratch1} USING GIST (geom)").format(**dbparams))

            gridsquare_index = 0
            for gridsquare_id in gridsquare_ids:
                gridsquare_index += 1

                self.log.info(f"[{self.node.name}] Using ST_Union to generate amalgamated grid square {gridsquare_index}/{len(gridsquare_ids)}")

                dbparams['gridsquare_id'] = sql.Literal(gridsquare_id)
                
                query_union_by_gridsquare = sql.SQL("""
                    INSERT INTO {output} (id, geom)
                        SELECT grid.id, (ST_Dump(ST_Union(ST_Intersection(grid.geom, dataset.geom)))).geom FROM {grid} grid
                        INNER JOIN {scratch1} dataset ON ST_Intersects(grid.geom, dataset.geom)
                        WHERE grid.id = {gridsquare_id} AND ST_GeometryType(dataset.geom) = 'ST_Polygon' 
                        GROUP BY grid.id
                """).format(**dbparams)
                self.postgis.execute_query(query_union_by_gridsquare)

            self.postgis.execute_query(sql.SQL("CREATE INDEX ON {output} USING GIST (geom)").format(**dbparams))
            self.postgis.drop_table(scratch_table_1)

            # Success Gate: Only update registry now
            if self.postgis.set_table_completed(self.node.output):
                self.log.info(f"[{self.node.name}] Amalgamate: COMPLETED")
                return True
            else:
                # This catches the bug where the node was never registered initially
                self.log.error(f"Amalgamate completed but registry record for {self.node.output} was not found.")
                return False

        except Error as e:
            self.log.error(f"[{self.node.name}] PostGIS error during amalgamation: {e}")
            return False
        except Exception as e:
            self.log.error(f"[{self.node.name}] Unexpected error: {e}")
            return False
