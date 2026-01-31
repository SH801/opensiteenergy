import os
import subprocess
import json
import logging
import sqlite3
from pathlib import Path
from opensite.processing.base import ProcessBase
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger
from opensite.postgis.opensite import OpenSitePostGIS

class OpenSiteImporter(ProcessBase):
    def __init__(self, node, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteImporter", log_level, shared_lock)
        self.base_path = OpenSiteConstants.DOWNLOAD_FOLDER
        self.postgis = OpenSitePostGIS(log_level)

    def get_projection(self, file_path, name):
        """
        Gets CRS of file
        Due to problems with CRS on some data sources, we have to adopt ad-hoc approach
        """

        if file_path.endswith('.gpkg'): 
            return self.get_gpkg_projection(file_path)

        if file_path.endswith('.geojson'): 
            
            # Check GeoJSON for crs
            # If missing and in Northern Ireland, use EPSG:29903
            # If missing and not in Northern Ireland, use EPSG:27700

            orig_srs = OpenSiteConstants.CRS_GEOJSON
            json_data = json.load(open(file_path))

            if 'crs' in json_data:
                orig_srs = json_data['crs']['properties']['name'].replace('urn:ogc:def:crs:', '').replace('::', ':').replace('OGC:1.3:CRS84', 'EPSG:4326')
            else:

                # DataMapWales' GeoJSON use EPSG:27700 even though default SRS for GeoJSON is EPSG:4326
                if name.endswith('--wales'): orig_srs = 'EPSG:27700'

                # Improvement Service GeoJSON uses EPSG:27700
                if name == 'local-nature-reserves--scotland': orig_srs = 'EPSG:27700'

                # Northern Ireland could be in correct GeoJSON without explicit crs (so EPSG:4326) or could be incorrect non-EPSG:4326 meters with non GB datum
                if name.endswith('--northern-ireland'): orig_srs = 'EPSG:29903'
                # ... so provide exceptions
                if name == 'world-heritage-sites--northern-ireland': orig_srs = 'EPSG:4326'

            return orig_srs

        return None

    def get_gpkg_projection(self, gpkg_path):
        """
        Gets projection of GPKG
        """

        if not Path(gpkg_path).exists(): return None

        with sqlite3.connect(gpkg_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("select a.srs_id from gpkg_contents as a;")
            result = cursor.fetchall()
            if len(result) == 0:
                self.log.error(f"{gpkg_path} has no layers - deleting")
                os.remove(gpkg_path)
                return None
            else:
                firstrow = result[0]
                return 'EPSG:' + str(dict(firstrow)['srs_id'])
  
    def run(self):
        """
        Imports spatial files into PostGIS, resolving variables if needed
        """

        if self.postgis.table_exists(self.node.output):
            self.log.info(f"[{self.node.output}] table already exists, skipping import")
            self.node.status = 'processed'
            return True

        input_file = self.node.input
        if input_file and input_file.startswith('VAR:'):
            # Lookup the value from our shared tracking (metadata/output variables)
            # This follows pattern for resolving dynamic node dependencies
            input_file_variable_name = input_file
            input_file = self.get_variable(input_file_variable_name)
            yaml_path = str(Path(self.base_path) / self.node.custom_properties['yml'])
            osm_export_tool_layer_name = self.get_top_variable(yaml_path)
            self.log.debug(f"Resolved osm-export-tool variable {input_file_variable_name} to layer name: {osm_export_tool_layer_name}")
        else:
            input_file = str(Path(self.base_path) / input_file)
        
        if not input_file or not os.path.exists(input_file):
            self.log.error(f"Import failed: File not found for input '{input_file}'")
            self.node.status = 'failed'
            return False

        # Connection and Validation
        pg_conn = self.postgis.get_ogr_connection_string()

        # Base ogr2ogr Command
        cmd = [
            "ogr2ogr",
            "-f", "PostgreSQL",
            pg_conn,
            input_file,
            "-makevalid",
            "-overwrite",
            "-lco", "GEOMETRY_NAME=geom",
            "-nln", self.node.output,
            "-nlt", "PROMOTE_TO_MULTI",
            "-skipfailures", 
            "-s_srs", self.get_projection(input_file, self.node.name), 
            "-t_srs", OpenSiteConstants.CRS_DEFAULT, 
            "--config", "PG_USE_COPY", "YES"
        ]

        sql_where_clause = None

        # Historic England Conservation Areas includes 'no data' polygons so remove as too restrictive
        if self.node.name == 'conservation-areas--england': sql_where_clause = "Name NOT LIKE 'No data%'"

        if sql_where_clause is not None:
            for extraitem in ["-dialect", "sqlite", "-sql", "SELECT * FROM '" + self.node.name + "' WHERE " + sql_where_clause]:
                cmd.append(extraitem)

        for extraconfig in ["--config", "OGR_PG_ENABLE_METADATA", "NO"]: cmd.append(extraconfig)

        # Format-Specific Logic
        if self.node.format == OpenSiteConstants.OSM_YML_FORMAT:
            # In ogr2ogr, the layer name follows the input file
            cmd.insert(5, osm_export_tool_layer_name)
            self.log.info(f"Importing OSM layer '{osm_export_tool_layer_name}' to '{self.node.output} from {os.path.basename(input_file)}")
        else:
            # For GeoJSON or other single-layer files, just set the table name
            self.log.info(f"Importing file {os.path.basename(input_file)} to table '{self.node.output}'")

        self.log.info(f"Executing PostGIS import for node: {self.node.name}")

        try:
            # Execute shell command
            subprocess.run(cmd, capture_output=True, text=True, check=True)

            postgis = OpenSitePostGIS()
            postgis.add_table_comment(self.node.output, self.node.name)

            # Success Gate: Only update registry now
            if self.postgis.set_table_completed(self.node.output):
                self.log.info(f"Import and registry update complete for {os.path.basename(input_file)} into table {self.node.output}")
                return True
            else:
                # This catches the bug where the node was never registered initially
                self.log.error(f"Import succeeded but registry record for {self.node.output} was not found.")
                return False
            
        except subprocess.CalledProcessError as e:
            self.log.error(f"PostGIS Import Error: {os.path.basename(input_file)} {e.stderr}")
            self.node.status = 'failed'
            return False