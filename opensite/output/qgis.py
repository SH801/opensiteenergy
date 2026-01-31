import logging
import os
import subprocess
from pathlib import Path
from opensite.output.base import OutputBase
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger
from dotenv import load_dotenv

load_dotenv()

class OpenSiteOutputQGIS(OutputBase):

    QGIS_PYTHON_PATH = '/usr/bin/python3'

    def __init__(self, node, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteOutputQGIS", log_level, shared_lock)
        self.base_path = OpenSiteConstants.OUTPUT_FOLDER
    
        if os.environ.get("QGIS_PYTHON_PATH") is not None: 
            self.QGIS_PYTHON_PATH = os.environ.get('QGIS_PYTHON_PATH')

    def run(self):
        """
        Runs QGIS output
        """

        python_path = Path(self.QGIS_PYTHON_PATH)
        qgis_output_path = Path(self.base_path) / self.node.output.replace('.qgis', 'qgs')

        if qgis_output_path.exists():
            self.log.info(f"{os.path.basename(qgis_output_path)} already exists, skipping creation")
            return True
        
        if not python_path.exists():
            self.log.error(f"Unable to locate QGIS Python at {self.QGIS_PYTHON_PATH}")
            self.log.error(" --> Edit your .env file to include the full path to QGIS's Python and rerun")
            self.log.error(" --> *** SKIPPING QGIS FILE CREATION ***")
            return False

        try:

            cmd = [self.QGIS_PYTHON_PATH, 'build-qgis.py', str(qgis_output_path)]
            subprocess.run(cmd, capture_output=True, text=True, check=True)

            self.log.info(f"[OpenSiteOutputQGIS] [{self.node.name}] COMPLETED")

            return True

        except subprocess.CalledProcessError as e:
            self.log.error(f"[OpenSiteOutputQGIS] [{self.node.name}] QGIS error {e.stderr}")
            return False
