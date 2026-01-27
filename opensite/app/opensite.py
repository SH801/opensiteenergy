import os
import json
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger
from opensite.cli.opensite import OpenSiteCLI
from opensite.ckan.opensite import OpenSiteCKAN
from opensite.model.graph.opensite import OpenSiteGraph
from opensite.queue.opensite import OpenSiteQueue

class OpenSiteApplication:
    def __init__(self, log_level=OpenSiteConstants.LOGGING_LEVEL):
        self._prepare_environment()
        self.log = OpenSiteLogger("OpenSite-App")
        self.log.info("Application initialized")
        self.log_level = os.getenv("OPENSITE_LOG_LEVEL", log_level)

    def _prepare_environment(self):
        """Creates required system folders defined in constants."""
        folders = OpenSiteConstants.ALL_FOLDERS
        for folder in folders:
            if not folder.exists():
                folder.mkdir(parents=True, exist_ok=True)

    def run(self):
        """
        Runs OpenSite application
        """

        # Initialise CLI
        cli = OpenSiteCLI(log_level=self.log_level) 

        # Initialize CKAN open data repository to use throughout
        # CKAN may or may not be used to provide site YML configuration
        ckan = OpenSiteCKAN(cli.get_current_value('ckan'))
        site_ymls = ckan.download_sites(cli.get_sites())

        # Initialize data model for session
        graph = OpenSiteGraph(cli.get_overrides(), log_level=self.log_level)
        graph.add_yamls(site_ymls)
        graph.update_metadata(ckan)

        # Generate all required processing steps
        graph.explode()

        if cli.get_preview():
            # Generate graph visualisation
            graph.generate_graph_preview(load=True)

        # Run processing queue
        queue = OpenSiteQueue(graph, log_level=self.log_level)
        queue.run()

        graph_list = graph.to_list()
        # osm_downloaders = []
        # for item in graph_list:
        #     if item['node_type'] == 'osm-downloader':
        #         osm_downloaders.append(item)
        # print(json.dumps(osm_downloaders, indent=4))

        # print(json.dumps(graph_list, indent=4))

    def shutdown(self, message="Process Complete"):
        """Clean exit point for the application."""
        self.log.info(message)