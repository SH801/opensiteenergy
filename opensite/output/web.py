import json
import logging
import shutil
from pathlib import Path
from opensite.output.base import OutputBase
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger

class OpenSiteOutputWeb(OutputBase):
    def __init__(self, node, log_level=logging.INFO, overwrite=False, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, overwrite=overwrite, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteOutputWeb", log_level, shared_lock)
        self.base_path = OpenSiteConstants.OUTPUT_FOLDER
    
    def run(self):
        """
        Runs Web output
        """

        # TO DO
        # 1. Download coastline to build
        # 2. Run tilemaker to generate basemap
        # 3. Generate different config files for gl-tileserver

        js_filepath = Path(self.base_path) / self.node.output
        
        try:
            # Copy main web index page to output folder
            shutil.copy('tileserver/index.html', str(Path(OpenSiteConstants.OUTPUT_FOLDER) / 'index.html'))

            with open(js_filepath, 'w', encoding='utf-8') as f:
                f.write(f"var opensite_layers = {json.dumps(self.node.custom_properties, indent=4)};")
            self.log.info(f"[OpenSiteOutputWeb] Data exported to {self.node.output}")

            return True
        
        except Exception as e:
            self.log.error(f"[OpenSiteOutputWeb] Export failed: {e}")
            return False
