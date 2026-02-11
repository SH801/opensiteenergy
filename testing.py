import os
import json
import shutil
import time
import os
import time
import logging
from psycopg2 import sql, Error
import datetime
from contextlib import asynccontextmanager
from pathlib import Path
from opensite.model.node import Node
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger
from opensite.cli.opensite import OpenSiteCLI
from opensite.ckan.opensite import OpenSiteCKAN
from opensite.model.graph.opensite import OpenSiteGraph
from opensite.queue.opensite import OpenSiteQueue
from opensite.postgis.opensite import OpenSitePostGIS
from opensite.processing.spatial import OpenSiteSpatial
from colorama import Fore, Style, init


class OpenSiteTest:
    def __init__(self, node, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        self.node = node
        self.log_level = log_level
        self.log = OpenSiteLogger("OpenSitTest", log_level, shared_lock)
        self.base_path = ""
        self.shared_lock = shared_lock
        self.shared_metadata = shared_metadata if shared_metadata is not None else {}
        self.postgis = OpenSitePostGIS(log_level)

    def parse_output_node_name(self, name):
        """
        Parses node name for output-focused nodes
        Nodes after amalgamate (postprocess, clip) are output-focused nodes and have slightly different names:
        [branch_name]--[normal_dataset_name]
        """

        name_elements = name.split('--')
        return {'name': '--'.join(name_elements[1:]), 'branch': name_elements[0]}

    def get_crs_default(self):
        """
        Get default CRS as number - for use in PostGIS
        """

        return OpenSiteConstants.CRS_DEFAULT.replace('EPSG:', '')

    def postprocess(self):
        """
        Postprocess node - join all grid squares together
        We assume each postprocess node has exactly one child, 
        ie. if postprocessing is needed on multiple children, insert amalgamate as single child 
        """

        name_elements = self.parse_output_node_name(self.node.name)
        self.node.name = name_elements['name']
        output_temp = f"{self.node.output}_temp"

        # Generate scratch table names
        def scratch(idx): return f"tmp_{idx}_{self.node.output}_{self.node.urn}"
        
        table_input = scratch(0) # We'll copy input here to add row_id
        table_seams = scratch(1) # Just the polygons touching edges
        table_islands = scratch(2) # Polygons safely away from edges
        table_welded = scratch(3) # The result of the union
        
        dbparams = {
            "crs": sql.Literal(self.get_crs_default()),
            "input": sql.Identifier(self.node.input),
            "output": sql.Identifier(self.node.output),
            "buffered_edges": sql.Identifier(OpenSiteConstants.OPENSITE_GRIDBUFFEDGES),
            "table_input": sql.Identifier(table_input),
            "table_seams": sql.Identifier(table_seams),
            "table_islands": sql.Identifier(table_islands),
            "table_welded": sql.Identifier(table_welded),
        }

        if self.postgis.table_exists(output_temp): self.postgis.drop_table(output_temp)

        if self.postgis.table_exists(self.node.output):
            self.log.info(f"[postprocess] [{self.node.output}] already exists, skipping postprocess")
            return True

        try:

            all_scratch_tables = [table_input, table_seams, table_islands, table_welded]

            def cleanup():
                for t in all_scratch_tables:
                    self.postgis.drop_table(t)

            cleanup()
            self.postgis.drop_table(self.node.output)

            # --- STEP 1: Materialize Input with Unique IDs ---
            self.log.info(f"[postprocess] [{self.node.name}] Step 1: Materializing indexed input...")
            start = datetime.datetime.now()
            self.postgis.execute_query(sql.SQL("""
                CREATE TABLE {table_input} AS 
                SELECT row_number() OVER () as row_id, * FROM {input};
                CREATE INDEX ON {table_input} (row_id);
                CREATE INDEX ON {table_input} USING GIST (geom);
            """).format(**dbparams))
            self.log.info(f"[postprocess] [{self.node.name}] Step 1: COMPLETED in {datetime.datetime.now() - start}")

            # --- STEP 2: Isolate Seam Candidates ---
            self.log.info(f"[postprocess] [{self.node.name}] Step 2: Extracting seam candidates...")
            start = datetime.datetime.now()
            self.postgis.execute_query(sql.SQL("""
            CREATE TABLE {table_seams} AS
                SELECT a.row_id, a.geom 
                FROM {table_input} a
                JOIN {buffered_edges} b ON ST_Intersects(a.geom, b.geom);
            CREATE INDEX ON {table_seams} (row_id);
            """).format(**dbparams))
            self.log.info(f"[postprocess] [{self.node.name}] Step 2: COMPLETED in {datetime.datetime.now() - start}")

            # --- STEP 3: Isolate Islands ---
            self.log.info(f"[postprocess] [{self.node.name}] Step 3: Isolating islands...")
            start = datetime.datetime.now()
            self.postgis.execute_query(sql.SQL("""
                CREATE TABLE {table_islands} AS
                SELECT a.* FROM {table_input} a
                LEFT JOIN {table_seams} b ON a.row_id = b.row_id
                WHERE b.row_id IS NULL;
            """).format(**dbparams))
            self.log.info(f"[postprocess] [{self.node.name}] Step 3: COMPLETED in {datetime.datetime.now() - start}")

            # --- STEP 4: Weld seams ---
            self.log.info(f"[postprocess] [{self.node.name}] Step 4: Unioning / welding seam geometries...")
            start = datetime.datetime.now()
            id_query = sql.SQL("SELECT row_id FROM {table_seams}").format(**dbparams)
            ids = [r['row_id'] for r in self.postgis.fetch_all(id_query)]
            total = len(ids)
            if total == 0:
                self.log.info(f"[postprocess] [{self.node.name}] No seams found. Creating empty table {dbparams['table_welded']}.")
                self.postgis.execute_query(sql.SQL("CREATE TABLE {table_welded} (geom geometry(Geometry, {crs}))").format(**dbparams))
            else:

                strategy = "ITERATIVE"

                self.log.info(f"[postprocess] [{self.node.name}] Strategy: {strategy} - {total} features")

                # EXECUTION: Conventional Path (Fast)
                if strategy == "CONVENTIONAL":
                    try:
                        self.postgis.execute_query(sql.SQL("""
                        CREATE TABLE {table_welded} AS 
                        SELECT (ST_Dump(ST_UnaryUnion(ST_Collect(ST_MakeValid(geom))))).geom::geometry(Polygon, {crs}) as geom
                        FROM {table_seams} 
                        WHERE row_id IN %s
                        """).format(**dbparams), (tuple(ids),))
                    except Exception as e:
                        self.log.warning(f"[postprocess] [{self.node.name}] Conventional weld failed: {e}")
                        self.log.warning(f"[postprocess] [{self.node.name}] Falling back to ITERATIVE strategy - slower but less memory intensive")
                        strategy = "ITERATIVE"
                        self.postgis.execute_query(sql.SQL("DROP TABLE IF EXISTS {table_welded}").format(**dbparams))

                batch_size = 50
                # EXECUTION: Iterative Path (Safe/Slow)
                if strategy == "ITERATIVE":

                    chunks = [ids[i : i + batch_size] for i in range(1, len(ids), batch_size)]

                    # Seed the table with first row
                    self.postgis.execute_query(sql.SQL("""
                    CREATE TABLE {table_welded} AS SELECT ST_MakeValid(geom) as geom FROM {table_seams} WHERE row_id = %s
                    """).format(**dbparams), (ids[0],))

                    for i, batch in enumerate(chunks):
                        self.postgis.execute_query(sql.SQL("""
                        UPDATE {table_welded} 
                        SET geom = ST_Union({table_welded}.geom, (SELECT ST_Union(ST_MakeValid(geom)) FROM {table_seams} WHERE row_id IN %s))
                        """).format(**dbparams), (tuple(batch),), autocommit=True)
                        
                        self.log.info(f"[postprocess] [{self.node.name}] Step 4: Progress: {(i+1) * batch_size}/{total} seams welded iteratively")

                        if i % 5 == 0:
                            self.postgis.execute_query(sql.SQL("VACUUM {table_welded}").format(**dbparams), autocommit=True)

            self.log.info(f"[postprocess] [{self.node.name}] Step 4: COMPLETED in {datetime.datetime.now() - start}")

            # --- STEP 5: Final assembly ---
            self.log.info(f"[postprocess] [{self.node.name}] Step 5: Finalizing output...")
            self.postgis.execute_query(sql.SQL("""
                CREATE TABLE {output} AS
                SELECT geom FROM {table_welded}
                UNION ALL
                SELECT geom FROM {table_islands};
                CREATE INDEX ON {output} USING GIST (geom);
            """).format(**dbparams))

            cleanup()
            self.log.info(f"[postprocess] [{self.node.name}] Success.")

            self.postgis.add_table_comment(self.node.output, self.node.name)
            self.postgis.register_node(self.node, None, name_elements['branch'])
            
            if self.postgis.set_table_completed(self.node.output):
                self.log.info(f"[postprocess] [{self.node.name}] COMPLETED")
                return True
            else:
                self.log.error(f"[postprocess] [{self.node.name}] Postprocess completed but registry record for {self.node.output} was not found.")
                return False

        except Exception as e:
            self.log.error(f"[postprocess] [{self.node.name}] Error during postprocess: {e}")
            return False
        
def main():

    node = Node(    urn=142, \
                    global_urn=1, \
                    name="test--test", \
                    title="Title", \
                    node_type=None, \
                    format=None, \
                    input="opensite_eacee1c8a648a10934738ee8ac18189e", \
                    action=None, \
                    output="opensite_cc8645cbc2b66be7aab1b6641a2c6807", \
                    custom_properties={})


    # node = Node(    urn=142, \
    #                 global_urn=1, \
    #                 name="test--test", \
    #                 title="Title", \
    #                 node_type=None, \
    #                 format=None, \
    #                 input="opensite_snapped", \
    #                 action=None, \
    #                 output="opensite_snapped", \
    #                 custom_properties={})

    app = OpenSiteTest(node, logging.INFO)
    app.postprocess()

if __name__ == "__main__":
    main()