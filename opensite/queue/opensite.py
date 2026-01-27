import json
import os
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import logging
import multiprocessing
import time
from typing import List
from opensite.logging.opensite import OpenSiteLogger
from opensite.model.node import Node
from opensite.constants import OpenSiteConstants
from opensite.download.opensite import OpenSiteDownloader
from opensite.processing.unzip import OpenSiteUnzipper
from opensite.processing.concatenate import OpenSiteConcatenator
from opensite.processing.run import OpenSiteRunner

class OpenSiteQueue:
    def __init__(self, graph, max_workers=None, log_level=logging.DEBUG):
        self.graph = graph
        self.action_groups = self.graph.get_action_groups()
        self.terminal_status = self.graph.get_terminal_status()
        self.log_level = log_level
        self.logger = OpenSiteLogger("OpenSiteQueue", self.log_level)

        # Resource Scaling
        self.cpus = os.cpu_count() or 1
        self.cpu_workers = max_workers or self.cpus
        self.io_workers = self.cpu_workers * 4  # Higher concurrency for network/disk
        
        self.graph.log.info(f"Processor ready. CPU Workers: {self.cpu_workers}, I/O Workers: {self.io_workers}")

    def _fetch_sizes_parallel(self, nodes: List[Node]):
        """Helper to fetch remote sizes for a list of nodes in parallel."""
        # Only check nodes that are downloads and don't have a cached size
        nodes_to_check = [
            n for n in nodes 
            if n.action == 'download' and not hasattr(n, '_remote_size')
        ]
        
        if not nodes_to_check:
            return

        def fetch_task(node):
            self.logger.info(f"Getting file size: {node.input}")
            downloader = OpenSiteDownloader()
            # This calls the logic we just fixed with 'identity' headers
            node._remote_size = downloader.get_remote_size(node)
            self.logger.info(f"File size {node._remote_size}: {node.input}")

        # Max 20 threads is usually a sweet spot for network I/O 
        # without triggering rate limits on most servers
        with ThreadPoolExecutor(max_workers=20) as executor:
            # list() forces the main thread to wait for all results
            list(executor.map(fetch_task, nodes_to_check))
        
        # Now the code only reaches this line once all threads are done
        self.logger.info("All file sizes fetched.")

    def get_runnable_nodes(self, actions=[], checkfilesizes=True) -> List[Node]:
        """
        Finds nodes ready for execution. 
        Ensures only one node per global_urn is added to the batch.
        """
        runnable = []
        seen_global_urns = set()
        
        # Get all node dictionaries from the graph
        node_dicts = self.graph.find_nodes_by_props({})
        
        for d in node_dicts:
            node = self.graph.find_node_by_urn(d['urn'])
            g_urn = node.global_urn

            # Skip if terminal or if we've already queued this global resource in this batch
            if node.status in self.terminal_status or g_urn in seen_global_urns:
                continue

            if len(actions) != 0:
                if node.action not in actions: continue

            # Check dependencies (children)
            children = getattr(node, 'children', [])
            if all(child.status == 'processed' for child in children):
                runnable.append(node)
                if g_urn:
                    seen_global_urns.add(g_urn)
        

        # Define the sort key (which now uses the cached values)
        def get_priority_weight(node: Node):
            is_download = (node.action == 'download')
            action_weight = 0 if is_download else 1
            
            try:
                format_weight = OpenSiteConstants.DOWNLOADS_PRIORITY.index(node.format)
            except (ValueError, AttributeError):
                format_weight = len(OpenSiteConstants.DOWNLOADS_PRIORITY) + 1
                
            # No network hit here! Just reading the cached _remote_size
            size_val = getattr(node, '_remote_size', 0)
            size_weight = -size_val if size_val and size_val > 0 else 0

            return (action_weight, format_weight, size_weight)

        # Order by file size using pre-fetch request (may not always work)
        if checkfilesizes:
            self._fetch_sizes_parallel(runnable)
            runnable.sort(key=get_priority_weight)

        return runnable

    def sync_global_status(self, node_urn: str, status: str):
        """
        Updates the target node and all its global 'clones' 
        to the specified status.
        """
        node = self.graph.find_node_by_urn(node_urn)
        g_urn = node.global_urn

        # Update the specific node
        node.status = status

        # Sync all clones sharing the same global_urn
        if g_urn:
            clones = self.graph.find_nodes_by_props({'global_urn': g_urn})
            for c_dict in clones:
                # Skip the one we just updated
                if c_dict['urn'] == node_urn:
                    continue
                c_node = self.graph.find_node_by_urn(c_dict['urn'])
                c_node.status = status

    @staticmethod
    def process_cpu_task(args):
        """
        Static wrapper for ProcessPoolExecutor. 
        Handles Amalgamate, Import, Buffer, and Run.
        """
        urn, name, title, node_type, format, input, action, output, custom_properties, log_level, shared_lock, shared_metadata = args
         
        logger = OpenSiteLogger("process_cpu_task", log_level, shared_lock)

        logger.info(f"[CPU:{action}] {name}")

        node = Node(urn=urn, name=name, title=title, node_type=node_type, format=format, input=input, action=action, output=output, custom_properties=custom_properties)

        try:

            if action == 'run':
                runner = OpenSiteRunner(node, log_level, shared_lock, shared_metadata)
                success = runner.run()

            if success: return urn, 'processed'
            else: return urn, 'failed'

        except Exception:
            return urn, 'failed'
        
    def process_io_task(self, node: Node, log_level, shared_lock, shared_metadata):
        """
        Standard method for ThreadPoolExecutor.
        Handles Download, Unzip, and Concatenate.
        """

        self.graph.log.info(f"[I/O:{node.action}] {node.name}")

        # Use shared_metadata for concatenator as needs access to cross-process variables

        try:
            success = False
            
            if node.action == 'download':
                downloader = OpenSiteDownloader(log_level, shared_lock)
                success = downloader.get(node)

            elif node.action == 'unzip':
                unzipper = OpenSiteUnzipper(node, log_level, shared_lock)
                success = unzipper.run()

            elif node.action == 'concatenate':
                concatenator = OpenSiteConcatenator(node, log_level, shared_lock, shared_metadata)
                success = concatenator.run()

            if success: return 'processed'
            else: return 'failed'

        except Exception as e:

            return 'failed'

    def run(self):
        """
        Main orchestration loop. Uses a continuous sweep to pipeline
        I/O and CPU tasks simultaneously.
        """
        self.graph.log.info(f"Starting orchestration with {self.io_workers} I/O threads and {self.cpu_workers} CPU processes.")
        
        # Track active futures: {future: urn}
        active_tasks = {}
        
        # Use a Manager for shared locks across processes
        manager = multiprocessing.Manager()
        shared_lock = manager.Lock()
        shared_metadata = manager.dict()

        # Keep executors open for the duration of the run to allow pipelining
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.io_workers) as io_exec, \
             concurrent.futures.ProcessPoolExecutor(max_workers=self.cpu_workers) as cpu_exec:
            
            while True:
                # print(json.dumps(dict(shared_metadata), indent=4))

                # 1. Get nodes that are ready to run (Dependencies met)
                ready_nodes = self.get_runnable_nodes(actions=['download', 'unzip', 'concatenate', 'run'], checkfilesizes=False)
                
                # Filter out nodes that are already currently in flight
                new_nodes = [n for n in ready_nodes if n.urn not in active_tasks.values()]

                # 2. Submit new tasks to the appropriate executor
                for node in new_nodes:
                    if node.action in self.action_groups['io_bound']:
                        future = io_exec.submit(self.process_io_task, node, self.log_level, shared_lock, shared_metadata)
                        active_tasks[future] = node.urn
                        self.graph.log.debug(f"Submitted I/O task: {node.name}")
                        
                    elif node.action in self.action_groups['cpu_bound']:
                        # Prepare the task args for the Process pool
                        task_args = (
                            node.urn, 
                            node.name, 
                            node.title,
                            node.node_type,
                            node.format,
                            node.input, 
                            node.action,
                            node.output,
                            node.custom_properties, 
                            self.log_level, 
                            shared_lock,
                            shared_metadata,
                        )
                        future = cpu_exec.submit(self.process_cpu_task, task_args)
                        active_tasks[future] = node.urn
                        self.graph.log.debug(f"Submitted CPU task: {node.name}")

                # 3. If no tasks are running and nothing is ready, check for completion or stalls
                if not active_tasks:
                    unfinished = [n for n in self.graph.find_nodes_by_props({}) 
                                 if n.get('status') not in ['completed', 'failed', 'skipped']]
                    
                    if not unfinished:
                        self.graph.log.info("Processing complete.")
                    else:
                        # If we have unfinished nodes but nothing is 'ready', we are stalled
                        self.graph.log.warning(f"Queue stalled. {len(unfinished)} nodes unreachable/blocked.")
                    break

                # 4. Wait for AT LEAST ONE task to complete
                # This is the "Pipelining Engine" - it yields as soon as any task finishes
                done, _ = concurrent.futures.wait(
                    active_tasks.keys(), 
                    timeout=1.0, # Brief timeout to allow periodic "Ready Node" re-scanning
                    return_when=concurrent.futures.FIRST_COMPLETED
                )

                # 5. Process completed tasks and update the graph
                for future in done:
                    urn = active_tasks.pop(future)
                    try:
                        # result for CPU tasks is (urn, status), for IO tasks usually just status
                        result = future.result()
                        # Normalize status extraction
                        status = result[1] if isinstance(result, tuple) else result
                        
                        self.sync_global_status(urn, status)
                        
                        # Generate preview to show incremental progress
                        self.graph.generate_graph_preview()
                        
                    except Exception as e:
                        self.graph.log.error(f"Task for URN {urn} generated an exception: {e}")
                        self.sync_global_status(urn, "failed")

                # Tiny sleep to prevent high CPU usage on the main thread
                time.sleep(0.05)