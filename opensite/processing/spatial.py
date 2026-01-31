import os
import subprocess
import hashlib
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
        
    def get_crs_default(self):
        """
        Get default CRS as number - for use in PostGIS
        """

        return OpenSiteConstants.CRS_DEFAULT.replace('EPSG:', '')
    
    def get_crs_output(self):
        """
        Get output CRS as number - for use in PostGIS
        """

        return OpenSiteConstants.CRS_OUTPUT.replace('EPSG:', '')

    def import_clipping_master(self):
        """
        Imports clipping master if not already imported
        """

        clipping_master_file = OpenSiteConstants.CLIPPING_MASTER
        clipping_temp_table = OpenSiteConstants.OPENSITE_CLIPPINGTEMP
        clipping_master_table = OpenSiteConstants.OPENSITE_CLIPPINGMASTER
        dbparams = {
            "crs": sql.Literal(self.get_crs_default()),
            'clipping_temp': sql.Identifier(clipping_temp_table),
            'clipping_master': sql.Identifier(clipping_master_table),
            "clipping_master_index": sql.Identifier(f"{clipping_master_table}_idx"),
        }
        query_create_clipping_master = sql.SQL("CREATE TABLE {clipping_master} (geom GEOMETRY(MultiPolygon, {crs}))").format(**dbparams)
        query_union_to_clipping_master = sql.SQL("INSERT INTO {clipping_master} SELECT ST_Union(geom) FROM {clipping_temp}").format(**dbparams)
        query_clipping_master_create_index = sql.SQL("CREATE INDEX {clipping_master_index} ON {clipping_master} USING GIST (geom)").format(**dbparams)

        if self.postgis.table_exists(clipping_master_table): return True

        self.log.info("[import_clipping_master] Importing clipping file")

        try:

            self.postgis.drop_table(clipping_temp_table)
            if not self.postgis.import_spatial_data(clipping_master_file, clipping_temp_table):
                self.log.error("[import_clipping_master] Unable to import clipping_master file to clipping_temp table")
                return False

            self.log.info("[import_clipping_master] Unioning clipping file and dropping temp table")

            self.postgis.execute_query(query_create_clipping_master)
            self.postgis.execute_query(query_union_to_clipping_master)
            self.postgis.execute_query(query_clipping_master_create_index)
            self.postgis.drop_table(clipping_temp_table)

            self.log.info("[import_clipping_master] Clipping file processing completed")

            return True
        except Error as e:
            self.log.error(f"[import_clipping_master] PostGIS error: {e}")
            return False
        except Exception as e:
            self.log.error(f"[import_clipping_master] Unexpected error: {e}")
            return False

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

        if self.postgis.table_exists(OpenSiteConstants.OPENSITE_GRIDPROCESSING):
            self.log.info("Processing grid already exists")
            self.get_processing_grid_square_ids()
            return True

        self.log.info(f"[create_processing_grid] Creating grid overlay with grid size {OpenSiteConstants.GRID_PROCESSING_SPACING} to reduce memory load during ST_Union")

        dbparams = {
            "crs": sql.Literal(self.get_crs_default()),
            "grid": sql.Identifier(OpenSiteConstants.OPENSITE_GRIDPROCESSING),
            "grid_index": sql.Identifier(f"{OpenSiteConstants.OPENSITE_GRIDPROCESSING}_idx"),
            "grid_spacing": sql.Literal(OpenSiteConstants.GRID_PROCESSING_SPACING),
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

            self.log.info(f"[create_processing_grid] Finished creating grid overlay with grid size {OpenSiteConstants.GRID_PROCESSING_SPACING}")

            return True
        except Error as e:
            self.log.error(f"[create_processing_grid] PostGIS Error during grid creation: {e}")
            return False
        except Exception as e:
            self.log.error(f"[create_processing_grid] Unexpected error: {e}")
            return False

    def create_output_grid(self):
        """
        Creates output grid to be used when creating mbtiles to improve performance and visual quality of mbtiles
        """

        if self.postgis.table_exists(OpenSiteConstants.OPENSITE_GRIDOUTPUT):
            self.log.info("Output grid already exists")
            return True

        self.log.info(f"[create_output_grid] Creating output grid with grid size {OpenSiteConstants.GRID_OUTPUT_SPACING} to improve performance and visual quality of mbtiles")

        dbparams = {
            "crs": sql.Literal(int(self.get_crs_default())),
            "grid": sql.Identifier(OpenSiteConstants.OPENSITE_GRIDOUTPUT),
            "grid_index": sql.Identifier(f"{OpenSiteConstants.OPENSITE_GRIDOUTPUT}_idx"),
            "grid_spacing": sql.Literal(OpenSiteConstants.GRID_OUTPUT_SPACING),
            "clipping_master": sql.Identifier(OpenSiteConstants.OPENSITE_CLIPPINGMASTER)
        }

        query_grid_create = sql.SQL("""
        CREATE TABLE {grid} AS 
            SELECT ST_Transform((ST_SquareGrid({grid_spacing}, ST_Transform(geom, 3857))).geom, {crs}) geom FROM {clipping_master}                                    
        """).format(**dbparams)
        query_grid_create_index = sql.SQL("CREATE INDEX {grid_index} ON {grid} USING GIST (geom)").format(**dbparams)

        try:
            self.postgis.execute_query(query_grid_create)
            self.postgis.execute_query(query_grid_create_index)

            self.log.info(f"[create_output_grid] Finished creating output grid with grid size {OpenSiteConstants.GRID_OUTPUT_SPACING}")

            return True
        except Error as e:
            self.log.error(f"[create_output_grid] PostGIS Error during grid creation: {e}")
            return False
        except Exception as e:
            self.log.error(f"[create_output_grid] Unexpected error: {e}")
            return False


    def get_processing_grid_square_ids(self):
        """
        Gets ids of all squares in processing grid
        """

        global PROCESSINGGRID_SQUARE_IDS

        if not PROCESSINGGRID_SQUARE_IDS:
            if not self.postgis.table_exists(OpenSiteConstants.OPENSITE_GRIDPROCESSING):
                self.log.error("Processing grid does not exist, unable to retrieve grid square ids")
                return None
            
            results = self.postgis.fetch_all(sql.SQL("SELECT id FROM {grid}").format(grid=sql.Identifier(OpenSiteConstants.OPENSITE_GRIDPROCESSING)))
            PROCESSINGGRID_SQUARE_IDS = [row['id'] for row in results]

        return PROCESSINGGRID_SQUARE_IDS

    def buffer(self):
        """
        Adds buffer to spatial dataset 
        Buffering is always added before dataset is split into grid squares
        """
            
        if self.postgis.table_exists(self.node.output):
            self.log.info(f"[buffer] [{self.node.output}] already exists, skipping buffer for {self.node.name}")
            self.node.status = 'processed'
            return True

        if 'buffer' not in self.node.custom_properties:
            self.log.error(f"[buffer] {self.node.name} is missing 'buffer' field, buffering failed")
            self.node.status = 'failed'
            return False
         
        buffer = self.node.custom_properties['buffer']
        input_table = self.node.input
        output_table = self.node.output

        self.log.info(f"[buffer] [{self.node.name}] Adding {buffer}m buffer to {input_table} to make {output_table}")

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
                self.log.info(f"[buffer] [{self.node.name}] Finished adding {buffer}m buffer to {input_table} to make {output_table}")
                return True
            else:
                # This catches the bug where the node was never registered initially
                self.log.error(f"[buffer] Buffer added but registry record for {self.node.output} was not found.")
                return False

        except Error as e:
            self.log.error(f"[buffer] [{self.node.name}] PostGIS error during buffer creation: {e}")
            return False
        except Exception as e:
            self.log.error(f"[buffer] [{self.node.name}] Unexpected error: {e}")
            return False

    def preprocess(self):
        """
        Preprocess node - dump to produce single geometry type then crop then finally split into grid squares
        """

        if self.postgis.table_exists(self.node.output):
            self.log.info(f"[preprocess] [{self.node.output}] already exists, skipping preprocess for {self.node.name}")
            self.node.status = 'processed'
            return True
    
        if not self.postgis.table_exists(OpenSiteConstants.OPENSITE_GRIDPROCESSING):
            self.log.info("[preprocess] Processing grid does not exist, creating it...")
            if not self.create_processing_grid():
                self.log.error(f"Failed to create processing grid, unable to preprocess {self.node.name}")
                self.node.status = 'failed'
                return False
            
        grid_table = OpenSiteConstants.OPENSITE_GRIDPROCESSING
        clip_table = OpenSiteConstants.OPENSITE_CLIPPINGMASTER
        gridsquare_ids = self.get_processing_grid_square_ids()
        scratch_table_1 = '_s1_' + self.node.output
        scratch_table_2 = '_s2_' + self.node.output
        scratch_table_3 = '_s3_' + self.node.output

        dbparams = {
            "crs": sql.Literal(self.get_crs_default()),
            "grid": sql.Identifier(grid_table),
            "clip": sql.Identifier(clip_table),
            "input": sql.Identifier(self.node.input),
            "scratch1": sql.Identifier(scratch_table_1),
            "scratch2": sql.Identifier(scratch_table_2),
            "scratch3": sql.Identifier(scratch_table_3),
            "output": sql.Identifier(self.node.output),
            "scratch1_index": sql.Identifier(f"{scratch_table_1}_idx"),
            "scratch2_index": sql.Identifier(f"{scratch_table_2}_idx"),
            "scratch3_index": sql.Identifier(f"{scratch_table_3}_idx"),
            "output_index": sql.Identifier(f"{self.node.output}_idx"),
        }

        # Drop scratch tables
        self.postgis.drop_table(scratch_table_1)
        self.postgis.drop_table(scratch_table_2)
        self.postgis.drop_table(scratch_table_3)

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
        query_s2_clip_1 = sql.SQL("""
        CREATE TABLE {scratch2} AS 
            SELECT ST_Intersection(clip.geom, data.geom) geom
            FROM {scratch1} data, {clip} clip 
            WHERE (NOT ST_Contains(clip.geom, data.geom) AND ST_Intersects(clip.geom, data.geom))
        """).format(**dbparams)
        query_s2_clip_2 = sql.SQL("""
        INSERT INTO {scratch2}  
            SELECT data.geom  
            FROM {scratch1} data, {clip} clip 
            WHERE ST_Contains(clip.geom, data.geom)
        """).format(**dbparams)
        query_s3_dump       = sql.SQL("CREATE TABLE {scratch3} AS SELECT (ST_Dump(geom)).geom geom FROM {scratch2}").format(**dbparams)
        query_output_create = sql.SQL("CREATE TABLE {output} (id INTEGER, geom GEOMETRY(Polygon, {crs}))").format(**dbparams)
        query_output_process_gridsquare = """
        INSERT INTO {output} 
            SELECT  grid.id, (ST_Dump(ST_Union(ST_Intersection(grid.geom, dataset.geom)))).geom geom 
            FROM {grid} grid, {scratch3} dataset 
            WHERE grid.id = {gridsquare_id} AND ST_geometrytype(dataset.geom) = 'ST_Polygon' GROUP BY grid.id"""
        query_s1_index      = sql.SQL("CREATE INDEX {scratch1_index} ON {scratch1} USING GIST (geom)").format(**dbparams)
        query_s2_index      = sql.SQL("CREATE INDEX {scratch2_index} ON {scratch2} USING GIST (geom)").format(**dbparams)
        query_s3_index      = sql.SQL("CREATE INDEX {scratch3_index} ON {scratch3} USING GIST (geom)").format(**dbparams)
        query_output_index  = sql.SQL("CREATE INDEX {output_index} ON {output} USING GIST (geom)").format(**dbparams)
        
        try:
            self.log.info(f"[preprocess] [{self.node.name}] Select only polygons, dump and make valid")

            self.postgis.execute_query(query_s1_dump_makevalid)
            self.postgis.execute_query(query_s1_index)

            self.log.info(f"[preprocess] [{self.node.name}] Clipping polygons [1] - Adding border-overlapping polygons")

            self.postgis.execute_query(query_s2_clip_1)

            self.log.info(f"[preprocess] [{self.node.name}] Clipping polygons [2] - Adding fully enclosed polygons")

            self.postgis.execute_query(query_s2_clip_2)
            self.postgis.execute_query(query_s2_index)

            self.log.info(f"[preprocess] [{self.node.name}] Dumping geometries")

            self.postgis.execute_query(query_s3_dump)
            self.postgis.execute_query(query_s3_index)

            self.log.info(f"[preprocess] [{self.node.name}] Creating preprocess table {self.node.output} by dissolving dataset")

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
                    self.log.info(f"[preprocess] [{self.node.name}] Processing grid square {gridsquares_index}/{gridsquares_count}")

                dbparams['gridsquare_id'] = sql.Literal(gridsquare_id)
                
                self.postgis.execute_query(sql.SQL(query_output_process_gridsquare).format(**dbparams))

            self.postgis.execute_query(query_output_index)

            self.postgis.drop_table(scratch_table_1)
            self.postgis.drop_table(scratch_table_2)
            self.postgis.drop_table(scratch_table_3)

            # Success Gate: Only update registry now
            if self.postgis.set_table_completed(self.node.output):
                self.log.info(f"[preprocess] [{self.node.name}] COMPLETED")
                return True
            else:
                # This catches the bug where the node was never registered initially
                self.log.error(f"[preprocess] Preprocess completed but registry record for {self.node.output} was not found.")
                return False

            return True
        except Error as e:
            self.log.error(f"[preprocess] [{self.node.name}] PostGIS error during buffer creation: {e}")
            return False
        except Exception as e:
            self.log.error(f"[preprocess] [{self.node.name}] Unexpected error: {e}")
            return False

    def amalgamate(self):
        """
        Amalgamates datasets into one
        Note: amalgamate is universally applied to all geographical subcomponents even if one subcomponent
        """

        self.node.output = self.get_variable(f"VAR:global_output_{self.node.global_urn}")

        if self.postgis.table_exists(self.node.output):
            self.log.info(f"[amalgamate] [{self.node.output}] already exists, skipping amalgamate")
            self.node.status = 'processed'
            return True

        if not self.postgis.table_exists(OpenSiteConstants.OPENSITE_GRIDPROCESSING):
            self.log.info("[amalgamate] Processing grid does not exist, creating it...")
            if not self.create_processing_grid():
                self.log.error(f"[amalgamate] Failed to create processing grid, unable to amalgamate {self.node.name}")
                self.node.status = 'failed'
                return False

        grid_table = OpenSiteConstants.OPENSITE_GRIDPROCESSING
        gridsquare_ids = self.get_processing_grid_square_ids()
        scratch_table_1 = '_s1_' + self.node.output
        children = self.node.custom_properties['children']

        dbparams = {
            "crs": sql.Literal(self.get_crs_default()),
            "grid": sql.Identifier(grid_table),
            "scratch1": sql.Identifier(scratch_table_1),
            "output": sql.Identifier(self.node.output),
            "scratch1_index": sql.Identifier(f"{scratch_table_1}_idx"),
            "output_index": sql.Identifier(f"{self.node.output}_idx"),
        }

        # Drop scratch tables
        self.postgis.drop_table(scratch_table_1)

        try:
            self.log.info(f"[amalgamate] [{self.node.name}] Starting amalgamation and dissolving")

            # Create output table regardless of number of children
            self.postgis.execute_query(sql.SQL("CREATE UNLOGGED TABLE {output} (id int, geom geometry(Geometry, {crs}))").format(**dbparams))
            self.postgis.add_table_comment(self.node.output, self.node.name)

            if len(children) == 1:

                dbparams['input'] = sql.Identifier(children[0])
                self.log.info(f"[{self.node.name}] Single child so directly copying from {children[0]} to {self.node.output}")
                self.postgis.execute_query(sql.SQL("INSERT INTO {output} SELECT * FROM {input}").format(**dbparams))

            else:

                # Create empty tables first using UNLOGGED for speed
                self.postgis.execute_query(sql.SQL("CREATE UNLOGGED TABLE {scratch1} (id int, geom geometry(Geometry, {crs}))").format(**dbparams))
        
                # Pour each child table in one by one
                child_index = 0
                for child in children:
                    child_index += 1
                    dbparams['input'] = sql.Identifier(child)
                    self.log.info(f"[amalgamate] [{self.node.name}] Amalgamating child table {child_index}/{len(children)}")
                    query_add_table = sql.SQL("INSERT INTO {scratch1} (id, geom) SELECT id, (ST_Dump(geom)).geom FROM {input}").format(**dbparams)
                    self.postgis.execute_query(query_add_table)

                self.postgis.execute_query(sql.SQL("CREATE INDEX ON {scratch1} USING GIST (geom)").format(**dbparams))

                gridsquare_index = 0
                for gridsquare_id in gridsquare_ids:
                    gridsquare_index += 1

                    self.log.info(f"[amalgamate] [{self.node.name}] Using ST_Union to generate amalgamated grid square {gridsquare_index}/{len(gridsquare_ids)}")

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
            self.postgis.add_table_comment(self.node.output, self.node.name)

            # Success Gate: Only update registry now
            # Register new table manually as output uses variable ()
            self.postgis.register_node(self.node)
            if self.postgis.set_table_completed(self.node.output):
                self.log.info(f"[amalgamate] [{self.node.name}] COMPLETED")
                return True
            else:
                # This catches the bug where the node was never registered initially
                self.log.error(f"[amalgamate] Amalgamate completed but registry record for {self.node.output} was not found.")
                return False

        except Error as e:
            self.log.error(f"[amalgamate] [{self.node.name}] PostGIS error during amalgamation: {e}")
            return False
        except Exception as e:
            self.log.error(f"[amalgamate] [{self.node.name}] Unexpected error: {e}")
            return False

    def generatehash(self, content):
        """
        Generates (semi-)unique database table name using hash from content
        """

        content_hash = hashlib.md5(content.encode()).hexdigest()

        return f"{OpenSiteConstants.DATABASE_GENERAL_PREFIX}{content_hash}"

    def parse_output_node_name(self, name):
        """
        Parses node name for output-focused nodes
        Nodes after amalgamate (postprocess, clip) are output-focused nodes and have slightly different names:
        [branch_name]--[normal_dataset_name]
        """

        name_elements = name.split('--')
        return {'name': '--'.join(name_elements[1:]), 'branch': name_elements[0]}

    def postprocess(self):
        """
        Postprocess node - join all grid squares together
        We assume each postprocess node has exactly one child, 
        ie. if postprocessing is needed on multiple children, insert amalgamate as single child 
        """

        # Set global variables if necessary
        input = self.get_variable(self.node.input)
        output = self.generatehash(f"{input}--postprocess")
        self.node.output = output
        self.set_output_variable(output, self.node.global_urn)

        # Convert output-focused name to normal name for registry listing
        name_elements = self.parse_output_node_name(self.node.name)
        self.node.name = name_elements['name']

        self.log.info(f"[postprocess] Running postprocess on {self.node.name} table {input}")

        if self.postgis.table_exists(output):
            self.log.info(f"[postprocess] [{output}] already exists, skipping postprocess")
            return True

        self.postgis.copy_table(input, output)

        self.postgis.add_table_comment(self.node.output, self.node.name)

        # Register new table manually as output uses variable ()
        self.postgis.register_node(self.node, None, name_elements['branch'])
        if self.postgis.set_table_completed(output):
            self.log.info(f"[postprocess] [{self.node.name}] COMPLETED")
            return True
        else:
            # This catches the bug where the node was never registered initially
            self.log.error(f"[postprocess] Postprocess completed but registry record for {self.node.output} was not found.")
            return False

    def clip(self):
        """
        Clips dataset to clipping path
        """

        # Set global variables if necessary
        input = self.get_variable(self.node.input)
        output = self.generatehash(f"{input}--clip-{self.node.custom_properties['clip']}")
        self.node.output = output
        self.set_output_variable(output, self.node.global_urn)

        # Convert output-focused name to normal name for registry listing
        name_elements = self.parse_output_node_name(self.node.name)
        self.node.name = name_elements['name']

        self.log.info(f"[clip] Running clip mask '{self.node.custom_properties['clip']}' on {self.node.name} table {input}")

        if self.postgis.table_exists(output):
            self.log.info(f"[clip] [{output}] already exists, skipping clip mask")
            return True

        self.postgis.copy_table(input, output)

        self.postgis.add_table_comment(self.node.output, self.node.name)

        # Register new table manually as output uses variable ()
        self.node.output = output
        self.postgis.register_node(self.node, None, name_elements['branch'])
        if self.postgis.set_table_completed(output):
            self.log.info(f"[clip] [{self.node.name}] COMPLETED")
            return True
        else:
            # This catches the bug where the node was never registered initially
            self.log.error(f"[clip] Clip completed but registry record for {self.node.output} was not found.")
            return False

        return True