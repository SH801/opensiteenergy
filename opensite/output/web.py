import logging
from opensite.output.base import OutputBase
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger

class OpenSiteOutputWeb(OutputBase):
    def __init__(self, node, log_level=logging.INFO, shared_lock=None, shared_metadata=None):
        super().__init__(node, log_level=log_level, shared_lock=shared_lock, shared_metadata=shared_metadata)
        self.log = OpenSiteLogger("OpenSiteOutputWeb", log_level, shared_lock)
        self.base_path = OpenSiteConstants.OUTPUT_FOLDER
    
    def run(self):
        """
        Runs Web output
        """

        return True