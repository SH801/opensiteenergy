import os
import subprocess
import logging
from pathlib import Path
from opensite.processing.base import ProcessBase
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger

class OpenSiteRunner(ProcessBase):
    def __init__(self, node, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteRunner", log_level, shared_lock)
        self.base_path = OpenSiteConstants.OSM_DOWNLOAD_FOLDER

    def run(self):
        """
        Executes command line tools like osm-export-tool via subprocess

        For osm-runner nodes, crucial fields are:

        input:  The YML file basename to use as mapping file. This should be in the OSM_DOWNLOAD_FOLDER, eg. osm-boundaries.yml
        output: The output GPKG that will be generated. This should be in the OSM_DOWNLOAD_FOLDER, eg. osm-boundaries.gpkg
        """

        if not self.node.input:
            self.log.error(f"Could not resolve input mapping for node {self.node.urn}")
            return False

        # Derive paths
        # Strip .gpkg to get base name (no '.gpkg' extension) for osm-export-tool as osm-export-tool requires this to run
        mapping_file                = self.base_path / self.node.input
        output_path                 = self.base_path / self.node.output
        output_basename             = self.node.output.rsplit('.gpkg', 1)[0]
        osm_export_output_param     = self.base_path / f"{output_basename}-tmp"
        output_tmp_path             = self.base_path / f"{output_basename}-tmp.gpkg"

        if output_tmp_path.exists(): output_tmp_path.unlink()

        if output_path.exists():
            self.log.info(f"{os.path.basename(str(output_path))} already exists, skipping osm-export-tool")
            return True
        
        osm_file = str(self.base_path / os.path.basename(self.node.custom_properties['osm']))

        if not self.node.input or not mapping_file.exists():
            self.log.error(f"Mapping file not resolved or missing: {self.node.input}")
            return False

        # Build the command list
        cmd = [
            "osm-export-tool",
            "-m", str(mapping_file),
            osm_file,
            str(osm_export_output_param)
        ]

        self.log.info(f"Executing osm-export-tool (note: long duration) - {self.node.input}")

        try:

            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True
            )

            os.replace(str(output_tmp_path), str(output_path))

            self.log.info(f"osm-export-tool successful. Created file at {os.path.basename(str(output_path))}")
            return True
        except subprocess.CalledProcessError as e:
            self.log.error(f"Shell execution failed: {e.stderr}")
            return False
