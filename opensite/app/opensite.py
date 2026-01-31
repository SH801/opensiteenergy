import os
import json
import shutil
import os
from pathlib import Path
from opensite.constants import OpenSiteConstants
from opensite.logging.opensite import OpenSiteLogger
from opensite.cli.opensite import OpenSiteCLI
from opensite.ckan.opensite import OpenSiteCKAN
from opensite.model.graph.opensite import OpenSiteGraph
from opensite.queue.opensite import OpenSiteQueue
from opensite.postgis.opensite import OpenSitePostGIS
from opensite.processing.spatial import OpenSiteSpatial

class OpenSiteApplication:
    def __init__(self, log_level=OpenSiteConstants.LOGGING_LEVEL):
        self.log = OpenSiteLogger("OpenSiteApplication")
        self.log.info("Application initialized")
        self.log_level = os.getenv("OPENSITE_LOG_LEVEL", log_level)

    def init_environment(self):
        """Creates required system folders defined in constants."""
        folders = OpenSiteConstants.ALL_FOLDERS
        for folder in folders:
            if not folder.exists():
                folder.mkdir(parents=True, exist_ok=True)

        spatial = OpenSiteSpatial(None)
        spatial.create_processing_grid()
        spatial.create_output_grid()

    def delete_folder(self, folder_path):
        """Deletes the specified directory and all its contents."""
        try:
            # We use ignore_errors=False to ensure we catch permission issues
            shutil.rmtree(folder_path)
            self.log.info(f"Successfully deleted: {folder_path}")
            return True
        except FileNotFoundError:
            self.log.warning(f"The folder {folder_path} does not exist.")
        except PermissionError:
            self.log.error(f"Error: Permission denied when trying to delete {folder_path}.")
        except Exception as e:
            self.log.error(f"An unexpected error occurred: {e}")

        return False
    
    def purgeall(self):
        """Purge all download files and opensite database tables"""

        self.purgedownloads()
        self.purgeoutputs()
        self.purgedb()

        self.log.info("[purgeall] completed")
        return True
    
    def purgedownloads(self):
        """Purge all download files"""

        download_folder = Path(OpenSiteConstants.DOWNLOAD_FOLDER).resolve()

        self.delete_folder(download_folder)
        self.log.info("[purgedownloads] completed")

        return True

    def purgeoutputs(self):
        """Purge all output files"""

        output_folder = Path(OpenSiteConstants.OUTPUT_FOLDER).resolve()

        self.delete_folder(output_folder)
        self.log.info("[purgeoutput] completed")

        return True

    def purgedb(self):
        """Purge all opensite database tables"""

        postgis = OpenSitePostGIS()
        postgis.purge_database()
        self.log.info("[purgedb] completed")

        return True

    def run(self):
        """
        Runs OpenSite application
        """

        RED = "\033[91m"
        BOLD = "\033[1m"
        RESET = "\033[0m"

        # Initialise CLI
        cli = OpenSiteCLI(log_level=self.log_level) 
        if cli.purgedb:
            print(f"\n{RED}{BOLD}{'='*60}")
            print(f"WARNING: You are about to delete all opensite tables")
            print(f"This includes registry, branch, and all spatial data tables.")
            print(f"{'='*60}{RESET}\n")
            
            confirm = input(f"Type {BOLD}'yes'{RESET} to delete all OpenSite data: ").strip().lower()
            if confirm == 'yes':
                self.purgedb()
                return
            print("Purge aborted. No tables were harmed.")

        if cli.purgeall:
            print(f"\n{RED}{BOLD}{'='*60}")
            print(f"WARNING: You are about to delete all downloads and opensite tables")
            print(f"This includes registry, branch, and all spatial data tables.")
            print(f"{'='*60}{RESET}\n")
            
            confirm = input(f"Type {BOLD}'yes'{RESET} to delete all downloads and OpenSite data: ").strip().lower()
            if confirm == 'yes':
                self.purgeall()
                return
            print("Purge aborted. No files or tables were harmed.")

        self.init_environment()

        # Initialize CKAN open data repository to use throughout
        # CKAN may or may not be used to provide site YML configuration
        ckan = OpenSiteCKAN(cli.get_current_value('ckan'))
        site_ymls = ckan.download_sites(cli.get_sites())

        # Initialize data model for session
        graph = OpenSiteGraph(  cli.get_overrides(), \
                                cli.get_outputformats(), \
                                cli.get_clip(), \
                                log_level=self.log_level)
        graph.add_yamls(site_ymls)
        graph.update_metadata(ckan)

        # Generate all required processing steps
        graph.explode()

        # Generate graph visualisation
        graph.generate_graph_preview(load=cli.get_preview())

        # Run processing queue
        queue = OpenSiteQueue(graph, log_level=self.log_level)
        queue.run()

        graph_list = graph.to_list()
        # for item in graph_list:
        #     print(item['urn'])
        # graph_list = graph.to_json()
        # print(json.dumps(graph_list, indent=4))

    def shutdown(self, message="Process Complete"):
        """Clean exit point for the application."""
        self.log.info(message)