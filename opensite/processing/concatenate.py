import yaml
import hashlib
import logging
import time
from pathlib import Path
from opensite.processing.base import ProcessBase
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger

class OpenSiteConcatenator(ProcessBase):
    def __init__(self, node, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteConcatenator", log_level)
        self.base_path = OpenSiteConstants.OSM_DOWNLOAD_FOLDER

    def run(self) -> bool:
        self.log.info(f"Concatenating OSM YAML files for {self.node.name}")
        
        try:
            # Collect inputs (paths relative to DOWNLOAD_FOLDER)
            download_path = Path(OpenSiteConstants.DOWNLOAD_FOLDER)
            input_paths = [(download_path / p).resolve() for p in self.node.input]

            merged_data = {}
            for p in input_paths:
                if p.exists():
                    with open(p, 'r', encoding='utf-8') as f:
                        data = yaml.safe_load(f)
                        if data: 
                            merged_data.update(data)
                else:
                    self.log.error(f"Source YAML not found: {p}")
                    return False
            
            # Hash and Write
            yaml_content = yaml.dump(merged_data, default_flow_style=False)
            osm_and_config = yaml_content + self.node.custom_properties['osm']
            content_hash = hashlib.sha256(osm_and_config.encode('utf-8')).hexdigest()[:16]
            final_filename = f"osm_config_{content_hash}.yml"
            final_path = str(Path(self.base_path) / final_filename)

            with open(final_path, 'w', encoding='utf-8') as f:
                f.write(yaml_content)

            # Use new helper to publish the truth
            # This automatically sets 'VAR:global_output_{self.node.global_urn}'
            self.set_output_variable(final_path)
            
            self.log.info(f"Successfully generated and registered: {final_filename}")
            return True

        except Exception as e:
            self.log.error(f"YAML Concatenation failed for {self.node.name}: {e}")
            return False