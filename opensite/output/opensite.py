import logging
import os
from pathlib import Path
from opensite.output.base import OutputBase
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger
from opensite.output.geojson import OpenSiteOutputGeoJSON
from opensite.output.gpkg import OpenSiteOutputGPKG
from opensite.output.mbtiles import OpenSiteOutputMbtiles
from opensite.output.shp import OpenSiteOutputSHP
from opensite.output.qgis import OpenSiteOutputQGIS
from opensite.output.web import OpenSiteOutputWeb

class OpenSiteOutput(OutputBase):
    def __init__(self, node, log_level=logging.INFO, overwrite=False, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, overwrite=overwrite, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteOutput", log_level, shared_lock)
        self.base_path = OpenSiteConstants.OUTPUT_FOLDER
    
    def run(self):
        """
        Runs output for every specific output type
        """

        outputObject = None

        if self.node.format == 'geojson':
            outputObject = OpenSiteOutputGeoJSON(self.node, self.log_level, self.overwrite, self.shared_lock, self.shared_metadata)

        if self.node.format == "gpkg":
            outputObject = OpenSiteOutputGPKG(self.node, self.log_level, self.overwrite, self.shared_lock, self.shared_metadata)

        if self.node.format == 'mbtiles':
            outputObject = OpenSiteOutputMbtiles(self.node, self.log_level, self.overwrite, self.shared_lock, self.shared_metadata)

        if self.node.format == 'shp':
            outputObject = OpenSiteOutputSHP(self.node, self.log_level, self.overwrite, self.shared_lock, self.shared_metadata)

        if self.node.format == 'qgis':
            outputObject = OpenSiteOutputQGIS(self.node, self.log_level, self.overwrite, self.shared_lock, self.shared_metadata)

        if self.node.format == 'web':
            outputObject = OpenSiteOutputWeb(self.node, self.log_level, self.overwrite, self.shared_lock, self.shared_metadata)

        if outputObject: return outputObject.run()

        return False
    