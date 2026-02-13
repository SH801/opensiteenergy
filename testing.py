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

        # Generate scratch table names
        def scratch(idx): return f"tmp_{idx}_{self.node.output}_{self.node.urn}"
        
        table_seams = scratch(0) # Just the polygons touching edges
        table_islands = scratch(1) # Polygons safely away from edges
        table_welded = scratch(2) # The result of the union
        
        dbparams = {
            "crs": sql.Literal(self.get_crs_default()),
            "input": sql.Identifier(self.node.input),
            "output": sql.Identifier(self.node.output),
            "buffered_edges": sql.Identifier(OpenSiteConstants.OPENSITE_GRIDBUFFEDGES),
            "table_seams": sql.Identifier(table_seams),
            "table_islands": sql.Identifier(table_islands),
            "table_welded": sql.Identifier(table_welded),
        }

        # if self.postgis.table_exists(self.node.output):
        #     self.log.info(f"[postprocess] [{self.node.output}] already exists, skipping postprocess")
        #     return True

        try:

            all_scratch_tables = [table_seams, table_islands, table_welded]

            def cleanup():
                for t in all_scratch_tables:
                    self.postgis.drop_table(t)

            cleanup()
            self.postgis.drop_table(self.node.output)

            # --- STEP 1: Isolate Seam Candidates ---
            self.log.info(f"[postprocess] [{self.node.name}] Step 1: Extracting seam candidates...")
            start = datetime.datetime.now()
            self.postgis.execute_query(sql.SQL("""
            CREATE TABLE {table_seams} AS
            SELECT a.geom AS geom FROM {input} a WHERE EXISTS (SELECT 1 FROM {buffered_edges} b WHERE ST_Intersects(a.geom, b.geom))""").format(**dbparams))
            self.log.info(f"[postprocess] [{self.node.name}] Step 1: COMPLETED in {datetime.datetime.now() - start}")

            # --- STEP 2: Isolate Islands ---
            self.log.info(f"[postprocess] [{self.node.name}] Step 2: Isolating islands...")
            start = datetime.datetime.now()
            self.postgis.execute_query(sql.SQL("""
            CREATE TABLE {table_islands} AS
            SELECT a.geom AS geom FROM {input} a WHERE NOT EXISTS (SELECT 1 FROM {buffered_edges} b WHERE ST_Intersects(a.geom, b.geom))""").format(**dbparams))
            self.log.info(f"[postprocess] [{self.node.name}] Step 2: COMPLETED in {datetime.datetime.now() - start}")

            # --- STEP 3: Weld seams ---
            self.log.info(f"[postprocess] [{self.node.name}] Step 3: Unioning / welding seam geometries...")
            start = datetime.datetime.now()
            strategy = "CONVENTIONAL"

            # EXECUTION: Conventional Path (Fast)
            if strategy == "CONVENTIONAL":
                self.log.info(f"[postprocess] [{self.node.name}] Strategy: {strategy}")
                try:
                    self.postgis.execute_query(sql.SQL("CREATE TABLE {table_welded} AS SELECT ST_Union(geom) AS geom FROM {table_seams}").format(**dbparams))
                except Exception as e:
                    self.log.warning(f"[postprocess] [{self.node.name}] Conventional weld failed - geometry too complex for PostGIS so copying over gridded data to target table")
                    strategy = "KEEPGRIDDED"
                    self.postgis.execute_query(sql.SQL("DROP TABLE IF EXISTS {table_welded}").format(**dbparams))

            # EXECUTION: Copy table_seams to table_welded unchanged
            # We tried but PostGIS unable to handle ST_Union on a dataset (possibly too many vertices)
            if strategy == "KEEPGRIDDED":
                self.log.info(f"[postprocess] [{self.node.name}] Strategy: {strategy}")
                try:
                    self.postgis.execute_query(sql.SQL("CREATE TABLE {table_welded} AS SELECT geom FROM {table_seams}").format(**dbparams))
                except Exception as e:
                    self.log.warning(f"[postprocess] [{self.node.name}] Unable to copy over gridded data to target table")
                    cleanup()
                    return False

            self.log.info(f"[postprocess] [{self.node.name}] Step 3: COMPLETED in {datetime.datetime.now() - start}")

            # --- STEP 4: Final assembly ---
            self.log.info(f"[postprocess] [{self.node.name}] Step 4: Finalizing output...")
            self.postgis.execute_query(sql.SQL("""
            CREATE TABLE {output} AS
            SELECT geom FROM {table_welded}
            UNION ALL
            SELECT geom FROM {table_islands};
            CREATE INDEX ON {output} USING GIST (geom);
            """).format(**dbparams))

            # cleanup()
            self.log.info(f"[postprocess] [{self.node.name}] Success")

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