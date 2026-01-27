import os
import subprocess
import json
import yaml
import hashlib
import logging
from pathlib import Path
from opensite.processing.base import ProcessBase
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger

class OpenSiteRunner(ProcessBase):
    def __init__(self, node, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteRunner", log_level, shared_lock)
        self.base_path = OpenSiteConstants.OSM_FOLDER

    def _get_env_python(self):
        """Locates the python binary inside the osm_export_tool conda env."""
        try:
            conda_base = subprocess.check_output(["conda", "info", "--base"]).decode().strip()
            python_path = os.path.join(conda_base, "envs", "osm_export_tool", "bin", "python")
            
            if not os.path.exists(python_path):
                raise FileNotFoundError(f"Conda environment python not found at {python_path}")
            
            return python_path
        except Exception as e:
            self.log.error(f"Failed to locate conda environment: {e}")
            return None

    def run(self):
        """Executes the osm-export-tool via subprocess."""

        # Resolve variables from shared_metadata
        # Assuming node.input is the URN of the concatenate/unzip task
        mapping_path = self.get_output_variable(self.node.input)
        if not mapping_path:
            self.log.error(f"Could not resolve input mapping for node {self.node.urn}")
            return False

        # Derive paths
        # Strip .yml to get base name for osm-export-tool
        output_base_file = mapping_path.rsplit('.yml', 1)[0]
        output_base_file_tmp = output_base_file + '-tmp'
        output_base_file_final = output_base_file + '.gpkg'
        output_base_file_temp_final = output_base_file_tmp + '.gpkg'
        self.set_output_variable(output_base_file_final)

        if os.path.exists(output_base_file_final):
            self.log.info(f"{os.path.basename(output_base_file_final)} already exists, skipping osm-export-tool")
            return True
        
        mapping_file = self.shared_metadata.get(self.node.input)
        osm_file = str(Path(OpenSiteConstants.OSM_FOLDER) / os.path.basename(self.node.custom_properties['osm']))

        if not mapping_file or not os.path.exists(mapping_file):
            self.log.error(f"Mapping file not resolved or missing: {mapping_file}")
            return False

        script_path = os.path.join(os.getcwd(), "osm-export-tool-run.sh")

        # Build the command list
        cmd = [
            "bash", script_path,
            mapping_file,
            osm_file,
            output_base_file_tmp
        ]

        self.log.info(f"Executing osm-export-tool-run.sh script for {mapping_file}")

        try:
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True
            )

            os.replace(output_base_file_temp_final, output_base_file_final)

            self.logger.info("Export successful")
            return True
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Shell execution failed: {e.stderr}")
            return False
