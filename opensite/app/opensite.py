import os
import json
import shutil
import time
import os
import threading
import uvicorn
import time
from datetime import datetime
from fastapi import FastAPI
from contextlib import asynccontextmanager
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
        self.log_level = os.getenv("OPENSITE_LOG_LEVEL", log_level)
        self.processing_start = time.time()
        self.server_thread = None
        self.log.info(f"{OpenSiteConstants.LOGGER_GREEN}{'='*60}{OpenSiteConstants.LOGGER_RESET}")
        self.log.info(f"{OpenSiteConstants.LOGGER_GREEN}{'*'*17} APPLICATION INITIALIZED {'*'*18}{OpenSiteConstants.LOGGER_RESET}")
        self.log.info(f"{OpenSiteConstants.LOGGER_GREEN}{'='*60}{OpenSiteConstants.LOGGER_RESET}")

        # self.stop_event = threading.Event()
        # self.app = self._setup_fastapi()
        # self.start_web_server()

    def _setup_fastapi(self):
        """Initializes the FastAPI app with routes."""
        app = FastAPI(title="OpenSite Graph API")

        @app.get("/nodes")
        async def get_nodes():
            return {"nodes": [n.to_dict() for n in self.graph.all_nodes]}

        @app.get("/health")
        async def health():
            return {"status": "running"}

        return app

    def start_web_server(self, host="127.0.0.1", port=8000):
        """Starts FastAPI in a background thread."""
        config = uvicorn.Config(self.app, host=host, port=port, log_level="info")
        server = uvicorn.Server(config)

        def run_server():
            # The server runs until the loop is stopped
            server.run()

        self.server_thread = threading.Thread(target=run_server, daemon=True)
        self.server_thread.start()
        self.log.info(f"[*] FastAPI started on http://{host}:{port}")
        self.log.info(f"[*] API Docs available at http://{host}:{port}/docs")

    def show_elapsed_time(self):
        """Shows elapsed time since object was created, ie. when process started"""

        processing_time = time.time() - self.processing_start
        processing_time_minutes = round(processing_time / 60, 1)
        processing_time_hours = round(processing_time / (60 * 60), 1)
        time_text = f"{processing_time_minutes} minutes ({processing_time_hours} hours) to complete"
        self.log.info(f"{OpenSiteConstants.LOGGER_YELLOW}Completed processing - {time_text}{OpenSiteConstants.LOGGER_RESET}")
        print("")

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
        self.purgeinstalls()
        self.purgetileserver()
        self.purgedb()

        self.log.info("[purgeall] completed")
        return True
    
    def purgetileserver(self):
        """Purge all tileserver files"""

        tileserver_folder = Path(OpenSiteConstants.TILESERVER_OUTPUT_FOLDER).resolve()

        self.delete_folder(tileserver_folder)
        self.log.info("[purgetileserver] completed")

        return True

    def purgeinstalls(self):
        """Purge all install files"""

        installs_folder = Path(OpenSiteConstants.INSTALL_FOLDER).resolve()

        self.delete_folder(installs_folder)
        self.log.info("[purgeinstalls] completed")

        return True

    def purgedownloads(self):
        """Purge all download files"""

        downloads_folder = Path(OpenSiteConstants.DOWNLOAD_FOLDER).resolve()

        self.delete_folder(downloads_folder)
        self.log.info("[purgedownloads] completed")

        return True

    def purgeoutputs(self):
        """Purge all output files"""

        outputs_folder = Path(OpenSiteConstants.OUTPUT_FOLDER).resolve()

        self.delete_folder(outputs_folder)
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

        if OpenSiteConstants.SERVER_BUILD:
            if Path(OpenSiteConstants.PROCESSING_COMPLETE_FILE).exists():
                self.log.info("Previous build run complete, aborting this run")
                exit()

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
            else:
                self.log.warning("Purge aborted. No tables were harmed.")

        if cli.purgeall:
            print(f"\n{RED}{BOLD}{'='*60}")
            print(f"WARNING: You are about to delete all downloads and opensite tables")
            print(f"This includes registry, branch, and all spatial data tables.")
            print(f"{'='*60}{RESET}\n")
            
            confirm = input(f"Type {BOLD}'yes'{RESET} to delete all downloads and OpenSite data: ").strip().lower()
            if confirm == 'yes':
                self.purgeall()
            else:
                self.log.warning("Purge aborted. No files or tables were harmed.")

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

        # If not '--graphonly', run processing queue
        queue = OpenSiteQueue(graph, log_level=self.log_level, overwrite=cli.get_overwrite())

        # Main processing loop
        if not cli.get_graphonly(): 

            # Change state files in case running in server environment
            with open(OpenSiteConstants.PROCESSING_START_FILE, 'w', encoding='utf-8') as file: 
                file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S,000 Processing started\n"))
            with open(OpenSiteConstants.PROCESSING_STATE_FILE, 'w') as file: file.write('PROCESSING')
            with open(OpenSiteConstants.PROCESSING_CMD_FILE, 'w') as file: file.write(cli.get_command_line())
            if Path(OpenSiteConstants.PROCESSING_COMPLETE_FILE).exists(): Path(OpenSiteConstants.PROCESSING_COMPLETE_FILE).unlink()
            
            queue.run()

            # Change state files in case running in server environment
            with open(OpenSiteConstants.PROCESSING_START_FILE, 'a', encoding='utf-8') as file: 
                file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S,000 Processing completed\n"))
            os.replace(OpenSiteConstants.PROCESSING_START_FILE, OpenSiteConstants.PROCESSING_COMPLETE_FILE)
            if Path(OpenSiteConstants.PROCESSING_STATE_FILE).exists(): Path(OpenSiteConstants.PROCESSING_STATE_FILE).unlink()

        # Show elapsed time at end
        self.show_elapsed_time()

    def shutdown(self, message="Process Complete"):
        """Clean exit point for the application."""

        self.stop_event.set()
        
        if self.graph:
            self.graph.cleanup_connections()

        self.log.info(message)