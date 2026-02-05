import os
import json
import shutil
import time
import os
import time
from datetime import datetime
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
from colorama import Fore, Style, init

init()

class OpenSiteApplication:
    def __init__(self, log_level=OpenSiteConstants.LOGGING_LEVEL):
        self.log = OpenSiteLogger("OpenSiteApplication")
        self.log_level = os.getenv("OPENSITE_LOG_LEVEL", log_level)
        self.processing_start = time.time()
        self.log.info(f"{Fore.GREEN}{'='*60}{Style.RESET_ALL}")
        self.log.info(f"{Fore.GREEN}{'*'*17} APPLICATION INITIALIZED {'*'*18}{Style.RESET_ALL}")
        self.log.info(f"{Fore.GREEN}{'='*60}{Style.RESET_ALL}")

    def show_elapsed_time(self):
        """Shows elapsed time since object was created, ie. when process started"""

        processing_time = time.time() - self.processing_start
        processing_time_minutes = round(processing_time / 60, 1)
        processing_time_hours = round(processing_time / (60 * 60), 1)
        time_text = f"{processing_time_minutes} minutes ({processing_time_hours} hours) to complete"
        self.log.info(f"{Fore.YELLOW}Completed processing - {time_text}{Style.RESET_ALL}")
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
        spatial.create_processing_grid_buffered_edges()

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
    
    def show_success_message(self, outputformats):
        """Gets final message text"""

        final_message = f"""
{Fore.MAGENTA + Style.BRIGHT}{'='*60}\n{'*'*10} OPEN SITE ENERGY BUILD PROCESS COMPLETE {'*'*9}\n{'='*60}{Style.RESET_ALL}
\nFinal layers created at:\n\n{Fore.CYAN + Style.BRIGHT}{OpenSiteConstants.OUTPUT_LAYERS_FOLDER}{Style.RESET_ALL}\n\n\n"""

        if 'web' in outputformats:
            final_message += f"""To view constraint layers as map, enter:\n\n{Fore.CYAN + Style.BRIGHT}./webview.sh{Style.RESET_ALL}\n\n\n"""
        
        if 'qgis' in outputformats:
            final_message += f"""QGIS file created at:\n\n{Fore.CYAN + Style.BRIGHT}{str(Path(OpenSiteConstants.OUTPUT_FOLDER) / OpenSiteConstants.OPENSITEENERGY_SHORTNAME)}.qgs{Style.RESET_ALL}\n\n"""

        print(final_message)

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

    def early_check_area(self, areas):
        """If boundaries table exist, check area is valid"""

        postgis = OpenSitePostGIS()
        if postgis.table_exists(OpenSiteConstants.OPENSITE_OSMBOUNDARIES):
            for area in areas:
                country = postgis.get_country_from_area(area)
                if country is None: return False
            return True
        return False

    def run(self):
        """
        Runs OpenSite application
        """

        if OpenSiteConstants.SERVER_BUILD:
            if Path(OpenSiteConstants.PROCESSING_COMPLETE_FILE).exists():
                self.log.info("Previous build run complete, aborting this run")
                exit()

        # Initialise CLI
        cli = OpenSiteCLI(log_level=self.log_level) 
        if cli.purgedb:
            print(f"\n{Fore.RED}{Style.BRIGHT}{'='*60}")
            print(f"WARNING: You are about to delete all opensite tables")
            print(f"This includes registry, branch, and all spatial data tables.")
            print(f"{'='*60}{Style.RESET_ALL}\n")
            
            confirm = input(f"Type {Style.BRIGHT}'yes'{Style.RESET_ALL} to delete all OpenSite data: ").strip().lower()
            if confirm == 'yes':
                self.purgedb()
            else:
                self.log.warning("Purge aborted. No tables were harmed.")

        if cli.purgeall:
            print(f"\n{Fore.RED}{Style.BRIGHT}{'='*60}")
            print(f"WARNING: You are about to delete all downloads and opensite tables")
            print(f"This includes registry, branch, and all spatial data tables.")
            print(f"{'='*60}{Style.RESET_ALL}\n")
            
            confirm = input(f"Type {Style.BRIGHT}'yes'{Style.RESET_ALL} to delete all downloads and OpenSite data: ").strip().lower()
            if confirm == 'yes':
                self.purgeall()
            else:
                self.log.warning("Purge aborted. No files or tables were harmed.")

        self.init_environment()

        # Attempt to check clipping area (if set) is valid
        if cli.get_clip():
            if not self.early_check_area(cli.get_clip()):
                self.log.error(f"At least one area in '{cli.get_clip()}' not found in boundary database, clipping will not be possible.")
                self.log.error(f"Please select the name of a different clipping area.")
                self.log.error(f"******** ABORTING ********")
                exit()

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

        # Generate initial processing graph
        graph.generate_graph_preview()

        # If not '--graphonly', run processing queue
        queue = OpenSiteQueue(graph, log_level=self.log_level, overwrite=cli.get_overwrite())

        # Main processing loop
        success = False
        if not cli.get_graphonly(): 

            # Change state files in case running in server environment
            with open(OpenSiteConstants.PROCESSING_START_FILE, 'w', encoding='utf-8') as file: 
                file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S,000 Processing started\n"))
            with open(OpenSiteConstants.PROCESSING_STATE_FILE, 'w') as file: file.write('PROCESSING')
            with open(OpenSiteConstants.PROCESSING_CMD_FILE, 'w') as file: file.write(cli.get_command_line())
            if Path(OpenSiteConstants.PROCESSING_COMPLETE_FILE).exists(): Path(OpenSiteConstants.PROCESSING_COMPLETE_FILE).unlink()
            
            success = queue.run(preview=cli.get_preview())

            # Change state files in case running in server environment
            with open(OpenSiteConstants.PROCESSING_START_FILE, 'a', encoding='utf-8') as file: 
                file.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S,000 Processing completed\n"))
            os.replace(OpenSiteConstants.PROCESSING_START_FILE, OpenSiteConstants.PROCESSING_COMPLETE_FILE)
            if Path(OpenSiteConstants.PROCESSING_STATE_FILE).exists(): Path(OpenSiteConstants.PROCESSING_STATE_FILE).unlink()

        # Show elapsed time at end
        self.show_elapsed_time()

        if success:
            self.show_success_message(cli.get_outputformats())

    def shutdown(self, message="Process Complete"):
        """Clean exit point for the application."""

        self.log.info(message)
