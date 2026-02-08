import json
import logging
import os
import subprocess
from pathlib import Path
from psycopg2 import sql, Error
from opensite.output.base import OutputBase
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger
from opensite.postgis.opensite import OpenSitePostGIS

class OpenSiteOutputMbtiles(OutputBase):
    def __init__(self, node, log_level=logging.INFO, overwrite=False, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, overwrite=overwrite, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteOutputMbtiles", log_level, shared_lock)
        self.base_path = OpenSiteConstants.OUTPUT_LAYERS_FOLDER
        self.postgis = OpenSitePostGIS(log_level)

    def run(self):
        """
        Runs Mbtiles output
        Creates grid clipped version of file to improve rendering and performance when used as mbtiles
        """

        tmp_output = f"tmp-{self.node.output.replace('.mbtiles', '.geojson')}" 
        tmp_output_path = Path(self.base_path) / tmp_output
        final_output_path = Path(self.base_path) / self.node.output
        grid_table = OpenSiteConstants.OPENSITE_GRIDOUTPUT
        scratch_table_1 = '_s1_' + str(self.node.urn)

        dbparams = {
            "crs": sql.Literal(self.get_crs_default()),
            "grid": sql.Identifier(grid_table),
            "input": sql.Identifier(self.node.input),
            "scratch1": sql.Identifier(scratch_table_1),
            "scratch1_index": sql.Identifier(f"{scratch_table_1}_idx"),
        }

        # Drop scratch tables
        self.postgis.drop_table(scratch_table_1)

        if tmp_output_path.exists(): tmp_output_path.unlink()

        query_s1_gridify = sql.SQL("""
        CREATE TABLE {scratch1} AS 
            SELECT (ST_Dump(ST_Intersection(layer.geom, grid.geom))).geom geom FROM {input} layer, {grid} grid
        """).format(**dbparams)
        query_s1_index      = sql.SQL("CREATE INDEX {scratch1_index} ON {scratch1} USING GIST (geom)").format(**dbparams)
        
        try:
            self.log.info(f"[OpenSiteOutputMbtiles] [{self.node.name}] Cutting up output into grid squares")

            dataset_name = self.node.output.replace('.mbtiles', '')
            self.postgis.execute_query(query_s1_gridify)
            self.postgis.execute_query(query_s1_index)
            self.postgis.export_spatial_data(scratch_table_1, dataset_name, str(tmp_output_path))
            self.postgis.drop_table(scratch_table_1)

            # Check for no features as GeoJSON with no features causes problem for tippecanoe
            # If no features, add dummy point so Tippecanoe creates mbtiles

            tmp_output_path_str = str(tmp_output_path)
            if os.path.getsize(tmp_output_path_str) < 1000:
                with open(tmp_output_path_str, "r") as json_file: geojson_content = json.load(json_file)
                if ('features' not in geojson_content) or (len(geojson_content['features']) == 0):
                    geojson_content['features'] = \
                    [
                        {
                            "type":"Feature", 
                            "properties": {}, 
                            "geometry": 
                            {
                                "type": "Point", 
                                "coordinates": [0,0]
                            }
                        }
                    ]
                    with open(tmp_output_path_str, "w") as json_file: json.dump(geojson_content, json_file)

            # Run Tippecanoe
            cmd = [
                "tippecanoe", 
                "-Z4", "-z15", 
                "-X", 
                "--generate-ids", 
                "--force", 
                "-n", dataset_name, 
                "-l", dataset_name, 
                tmp_output_path_str, 
                "-o", str(final_output_path) 
            ]

            subprocess.run(cmd, capture_output=True, text=True, check=True)

            if tmp_output_path.exists(): os.remove(str(tmp_output_path))

            self.log.info(f"[OpenSiteOutputMbtiles] [{self.node.name}] COMPLETED")

            return True

        except subprocess.CalledProcessError as e:
            self.log.error(f"[OpenSiteOutputMbtiles] [{self.node.name}] Tippecanoe error {e.stderr}")
            return False
        except Error as e:
            self.log.error(f"[OpenSiteOutputMbtiles] [{self.node.name}] PostGIS error during gridify: {e}")
            return False
        except Exception as e:
            self.log.error(f"[OpenSiteOutputMbtiles] [{self.node.name}] Unexpected error: {e}")
            return False

        return False