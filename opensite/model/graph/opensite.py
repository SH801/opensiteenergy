import copy
import os
import json
import yaml
import logging
from typing import Dict, Any, List, Optional
from .base import Graph
from ..node import Node
from opensite.constants import OpenSiteConstants
from opensite.postgis.opensite import OpenSitePostGIS
from opensite.logging.opensite import OpenSiteLogger
from opensite.ckan.opensite import OpenSiteCKAN

class OpenSiteGraph(Graph):

    TREE_BRANCH_PROPERTIES = OpenSiteConstants.TREE_BRANCH_PROPERTIES

    def __init__(self, overrides=None, log_level=logging.INFO):
        super().__init__(overrides)

        self.log = OpenSiteLogger("OpenSiteGraph", log_level)
        self.db = OpenSitePostGIS()
        self.db.sync_registry()

        self.log.info("Graph initialized and ready.")
        
    def register_to_database(self):
        """Syncs the graph structure to PostGIS using the logger for feedback."""
        self.log.info("Starting database synchronization...")

        def _recurse_and_register(node, branch):
            # Use debug for high-volume mapping logs (White)
            self.log.debug(f"Mapping node: {node.name} -> {node.output}")
            self.db.register_node(node, branch)
            
            for child in node.children:
                _recurse_and_register(child, branch)

        for branch in self.root.children:
            yml_hash = branch.custom_properties.get('hash')
            if yml_hash:
                # Use info for major milestones (Blue)
                self.log.info(f"Syncing branch: {branch.name} [{yml_hash[:8]}]")
                
                try:
                    self.db.register_branch(branch.name, yml_hash, branch.custom_properties)
                    _recurse_and_register(branch, branch)
                except Exception as e:
                    # Use error for failures (Red)
                    self.log.error(f"Failed to sync branch {branch.name}: {e}")

        self.log.info("Database synchronization complete.")

    def convert_name_to_title(self, name: str) -> str:
        """
        'railway-lines--uk' -> 'Railway Lines'
        'hazard-zone--exclusion--restricted' -> 'Hazard Zone - Exclusion - Restricted'
        """

        # REDUNDANT - We use CKAN to get correct title
        lowercase_words = [" And ", " Of ", " From "]

        if not name:
            return ""

        delete_area = ['uk', 'gb', 'eu']

        # Split on double hyphen
        parts = name.split("--")
        
        # Filter and process
        cleaned_parts = []
        for part in parts:
            if part.lower() not in delete_area:
                # Replace single hyphens with spaces
                words = part.replace("-", " ").split()
                capitalized_words = " ".join([w.capitalize() for w in words])
                cleaned_parts.append(capitalized_words)

        title = " - ".join(cleaned_parts)
        for lowercase_word in lowercase_words: title = title.replace(lowercase_word, lowercase_word.lower())

        return title

    def get_math_context(self, branch: Node) -> Dict[str, float]:
        """
        Builds a math context specific to the properties 
        stored on this branch's root node.
        """
        all_props = branch.custom_properties
        function_keys = self.TREE_BRANCH_PROPERTIES.get('functions', [])
        
        return {
            k: v for k, v in all_props.items() 
            if k in function_keys and isinstance(v, (int, float, str))
        }

    def add_yaml(self, filepath: str):
        """Loads a YAML file and triggers the branch-specific enrichment logic."""
        # 1. Use the base class to load the raw file structure into a branch
        super().add_yaml(filepath)
        
        if not self.root.children: return False
            
        # 2. Get the branch we just created (the last child of the root)
        current_branch = self.root.children[-1]

        # 3. Trigger the unified enrichment logic
        # This now handles property mapping, math, and surgical pruning
        self.enrich_branch(current_branch)

        self.register_to_database()

        return True

    def resolve_branch_math(self, branch: Node):
        # Get context dynamically via our new function
        context = self.get_math_context()
        
        def walk(node: Node):
            for k, v in node.custom_properties.items():
                if isinstance(v, str):
                    # base.py's resolve_math handles the calculation
                    node.custom_properties[k] = self.resolve_math(v, context)
            
            for child in node.children:
                walk(child)

        walk(branch)

    def enrich_branch(self, branch: Node):
        """
        Merges file data with global defaults and prunes
        """
        
        self.log.debug(f"Running enrich_branch")

        all_registry_keys = [k for sub in self.TREE_BRANCH_PROPERTIES.values() for k in sub]

        for key in all_registry_keys:
            # 1. Try to find the node in the current YAML branch
            prop_node = self.find_child(branch, key)
            
            # 2. Determine value: Local Node > Global Default
            val = None
            if prop_node:
                val = prop_node.custom_properties.get('value')
            else:
                val = self._defaults.get(key)

            # 3. Apply to Branch Node
            if val is not None:
                if key == 'title':
                    branch.title = val
                else:
                    branch.custom_properties[key] = val

        # 3. Get math context FROM the branch properties we just set
        context = self.get_math_context(branch)
        
        # 4. Locate structure/style/buffer roots
        struct_root = self.find_child(branch, "structure")
        style_root = self.find_child(branch, "style")
        buffer_root = self.find_child(branch, "buffers")

        if not struct_root:
            # Cleanup if no structure (deletes osm, tip-height nodes etc.)
            for child in list(branch.children):
                self.delete_node(child)
            return

        # 5. Enrichment Loop (Math & Style)
        for category_node in struct_root.children:
            category_node.node_type = 'group'
            
            # Apply Style
            if style_root:
                style_match = self.find_child(style_root, category_node.name)
                if style_match:
                    category_node.style = {
                        c.name: c.custom_properties.get('value') 
                        for c in style_match.children
                    }

            # Determine parents, apply buffers and resolve math
            for dataset_node in category_node.children:
                dataset_node.node_type = "source"
                if '--' in dataset_node.name:
                    dataset_node.custom_properties['parent'] = dataset_node.name.split("--")[0]

                if buffer_root:
                    buf_node = self.find_child(buffer_root, dataset_node.name)
                    if buf_node:
                        val = buf_node.custom_properties.get('value')
                        dataset_node.database_action = "buffer"
                        # Math resolution uses the branch-specific context
                        dataset_node.custom_properties['buffer_value'] = self.resolve_math(val, context)

            

        # 6. Sibling Cleanup
        # Deletes all original YAML nodes (tip-height, title, style, etc.)
        extraneous_nodes = self.get_siblings(struct_root)
        for node in extraneous_nodes:
            self.delete_node(node)

        # 7. Final Promotion
        valid_data_nodes = list(struct_root.children)
        for node in valid_data_nodes:
            node.parent = branch
            branch.children.append(node)

        self.delete_node(struct_root)

        # self._apply_titles_recursive(branch)

    def _apply_titles_recursive(self, node: Node):
        """Walks down the graph and sets titles if they are currently just the name."""

        # If title is missing or still matches the raw name, format it
        if not node.title or node.title == node.name:
            node.title = self.convert_name_to_title(node.name)
        
        self.log.debug(f"Running _apply_titles_recursive: {node.name} --> '{node.title}'")

        for child in node.children:
            self._apply_titles_recursive(child)

    def choose_priority_resource(self, resources, priority_ordered_formats):
        """
        Choose the single best dataset from a list based on FORMATS priority.
        """
        if not resources:
            return None
        
        # We want to find the dataset whose resource format has the lowest index in self.FORMATS
        best_resource = resources[0] # Default to first if no priority match found
        best_index = len(priority_ordered_formats)

        for resource in resources:
            format = resource.get('format')
            if format in priority_ordered_formats:
                current_index = priority_ordered_formats.index(format)
                if current_index < best_index:
                    best_index = current_index
                    best_resource = resource
                    
                    # Optimization: If we found the #1 priority (GPKG), we can stop looking
                    if best_index == 0:
                        return best_resource

        return best_resource
        
    def update_metadata(self, ckan: OpenSiteCKAN):
        """
        Syncs titles and URLs for both Groups and Datasets across the entire graph.
        """
        self.log.info("Synchronizing node titles with CKAN metadata...")

        model = ckan.query()
        ckan_base = ckan.url

        # Build a unified lookup map for both Groups and Datasets
        ckan_lookup = {}
        
        for group_name, data in model.items():
            # Add the group itself to the lookup (if it's not the 'default' catch-all)
            # This allows folders in your graph to get their Titles from CKAN groups
            if group_name != 'default':
                ckan_lookup[group_name] = {
                    'title': data.get('group_title', group_name).strip(),
                }

            # Add priority resource within each dataset
            for dataset in data.get('datasets', []):
                priority_resource = self.choose_priority_resource(dataset.get('resources', []), ckan.FORMATS)
                package_name = dataset.get('package_name', '')
                if package_name:
                    ckan_lookup[package_name] = {
                        'title': dataset.get('title').strip(), 
                        'input': priority_resource.get('url').strip(),
                        'format': priority_resource.get('format').strip()
                    }
                
        # Recursive walker (unchanged logic, now with better data)
        def walk_and_update(node):
            matches = 0
            if node.name in ckan_lookup:
                meta = ckan_lookup[node.name]
                node.title = meta['title']
                if 'input' in meta: node.input = meta['input']
                if 'format' in meta: node.format = meta['format'] 
                matches += 1
            
            if hasattr(node, 'children'):
                for child in node.children:
                    matches += walk_and_update(child)
            return matches

        # Execute
        total_matches = walk_and_update(self.root)
        self.log.info(f"Metadata sync complete. Updated {total_matches} total nodes.")

    def capture_core_structure(self):
        """
        Creates a deep copy of the current root hierarchy and stores it 
        in self.corestructure to preserve the 'unexploded' state.
        """
        self.log.info("Capturing snapshot of the core graph structure.")
        
        # deepcopy replicates the entire tree (nodes and their children lists)
        # so that modifications to the main graph won't affect the snapshot.
        self.corestructure = copy.deepcopy(self.root)

    def explode(self):
        """
        Builds processing graph
        """

        # Take the snapshot first
        self.capture_core_structure()

        # Groups datasets with same initial slug together, 'national-parks--england', 'national-parks--scotland', etc
        self.add_parents()

        # Generate download nodes
        self.add_downloads()

        # Generate unzipping nodes
        self.add_unzips()

        # Generate osm-export-tool nodes
        self.add_osmexporttool_nodes()

    def add_parents(self):
        """
        Groups sibling nodes, derives the group title from children, 
        and sets action to 'amalgamate'.
        """
        self.log.info("Organizing graph hierarchy and setting 'amalgamate' actions...")
        
        def process_node(current_node):
            if hasattr(current_node, 'children') and current_node.children:
                for child in list(current_node.children):
                    process_node(child)

            if not hasattr(current_node, 'children') or not current_node.children:
                return

            group_map = {}
            for child in current_node.children:
                props = getattr(child, 'custom_properties', {}) or {}
                parent_val = props.get('parent')
                
                if parent_val:
                    if parent_val not in group_map:
                        group_map[parent_val] = []
                    group_map[parent_val].append(child)

            for group_name, siblings in group_map.items():
                child_urns = [s.urn for s in siblings]
                
                # 1. Title Logic: Inherit from first child
                ref_child = siblings[0]
                original_title = getattr(ref_child, 'title', ref_child.name)
                
                if original_title and ' - ' in original_title:
                    parts = original_title.split(' - ')
                    group_title = ' - '.join(parts[:-1])
                else:
                    group_title = group_name.replace('-', ' ').title()
                
                # 2. Create the node with numeric URN
                new_group = self.create_group_node(
                    parent_urn=current_node.urn,
                    child_urns=child_urns,
                    group_name=group_name,
                    group_title=group_title
                )
                
                # 3. Apply metadata
                if new_group:
                    new_group.node_type = 'group'
                    new_group.action = 'amalgamate'  # <--- Set the action here
                    self.log.debug(f"Created group '{group_title}' (URN: {new_group.urn}) with action 'amalgamate'")

        process_node(self.root)

    def add_downloads(self):
        """
        Identifies terminal nodes with remote inputs and inserts a 
        'download' node as a child. Sets local file paths for the parent.
        """
        
        self.log.info("Adding download nodes for remote datasources...")
        
        # 1. Gather all current terminal nodes from base helper
        terminals = self.get_terminal_nodes()

        for node in terminals:
            # 2. Check for remote input
            input_url = getattr(node, 'input', '')
            if isinstance(input_url, str) and input_url.startswith('http'):
                
                # 3. Determine extension using OpenSiteConstants
                node_format = getattr(node, 'format', 'Unknown')
                extension = OpenSiteConstants.CKAN_FILE_EXTENSIONS.get(node_format, 'ERROR')
                
                # 4. Create unique URN and instantiate the child
                download_urn = self.get_new_urn()
                download_name = f"{node.name}"
                download_title = f"Download - {node.title}"
                
                download_node = Node(
                    name=download_name, 
                    title=download_title, 
                    urn=download_urn
                )

                # 5. Configure Download node properties
                download_node.node_type = 'download'
                download_node.custom_properties = {}
                download_node.input = node.input  # Move the URL to the child
                download_node.output = f"{node.name}.{extension}" # Local filename
                download_node.format = node.format
                download_node.action = 'download'

                # 6. Re-wire the Parent: 
                # Parent now expects the local file generated by the child
                node.input = download_node.output
                
                if not hasattr(node, 'children'):
                    node.children = []
                node.children.append(download_node)
                
                self.log.debug(
                    f"Added download (URN: {download_urn}) for parent (URN: {node.urn})"
                )

    def add_unzips(self):
        """
        Searches for download nodes with .zip URLs and inserts 
        an 'unzip' step into the pipeline.
        """
        self.log.info("Checking for zip archives to extract...")
        
        # 1. We look for terminal nodes (which should be our 'download' nodes now)
        terminals = self.get_terminal_nodes()

        for node in terminals:
            input_url = getattr(node, 'input', '')
            
            # 2. Check if the basename of the URL ends in .zip
            if isinstance(input_url, str) and input_url.lower().split('?')[0].endswith('.zip'):
                
                # 3. Clone the node to create the 'Download' child
                # We use get_new_urn to keep IDs unique
                zip_child_urn = self.get_new_urn()
                
                zip_child = Node(
                    name=f"{node.name}-file",
                    title=node.title, # Keep original title for the actual download
                    urn=zip_child_urn
                )
                
                # 4. Define the Zip Basename
                # If node.output is 'residential.yml', zip_output is 'residential.yml.zip'
                zip_output = f"{node.output}.zip"

                # 5. Configure the Child (The Downloader)
                zip_child.node_type = 'download'
                zip_child.action = 'download'
                zip_child.input = node.input   # Child takes the remote URL
                zip_child.output = zip_output  # Child saves the .zip file
                zip_child.format = node.format
                zip_child.custom_properties = {}

                # 6. Configure the Parent (The Unzipper)
                node.node_type = 'process'
                node.action = 'unzip'
                node.title = f"Unzip - {node.title}"
                node.input = zip_output        # Parent takes the .zip from child
                # node.output stays as the unzipped filename (e.g., .yml or .gpkg)
                
                # 7. Re-parenting
                if not hasattr(node, 'children'):
                    node.children = []
                node.children.append(zip_child)
                
                self.log.debug(f"Inserted unzip step for {zip_output} (URN: {node.urn})")

    def add_osmexporttool_nodes(self):
        """
        Builds the OSM stack: Runner is the parent, with Downloader 
        and Concatenator as siblings beneath it.
        """
        self.log.info("Splicing OSM stack: Adding Downloader as sibling to Concatenator...")

        # 1. Query for the base YML download nodes
        yml_node_dicts = self.find_nodes_by_props({
            'format': OpenSiteConstants.OSM_YML_FORMAT, 
            'node_type': 'download'
        })
        
        if not yml_node_dicts:
            return

        # 2. Group by lineage-baked 'osm' URL
        groups = {}
        for d in yml_node_dicts:
            node = self.find_node_by_urn(d['urn'])
            osm_url = self.get_property_from_lineage(node.urn, 'osm')
            if not osm_url:
                continue
            if osm_url not in groups:
                groups[osm_url] = []
            groups[osm_url].append(node)

        # 3. Process each unique OSM source group
        for osm_url, group_nodes in groups.items():
            group_outputs = sorted(list(set(n.output for n in group_nodes if n.output)))
            osm_url_basename = os.path.basename(osm_url)
            
            concat_gurn = self.get_new_global_urn()
            run_gurn = self.get_new_global_urn()
            down_gurn = self.get_new_global_urn()

            for node in group_nodes:
                # --- LAYER 1: Concatenator ---
                concat_node = Node(
                    name=f"osm-consolidator--{osm_url}",
                    title=f"Concatenate OSM Configs - {osm_url_basename}",
                    urn=self.get_new_urn()
                )
                concat_node.global_urn = concat_gurn
                concat_node.action = 'concatenate'
                concat_node.node_type = 'osm-concatenator'
                concat_node.input = group_outputs 
                
                # --- LAYER 2: Downloader ---
                down_node = Node(
                    name=f"osm-downloader--{osm_url}",
                    title=f"Download OSM Source - {osm_url_basename}",
                    urn=self.get_new_urn()
                )
                down_node.global_urn = down_gurn
                down_node.action = 'download'
                down_node.node_type = 'osm-downloader'
                down_node.input = osm_url

                # --- LAYER 3: Runner ---
                run_node = Node(
                    name=f"osm-runner--{osm_url}",
                    title=f"Run OSM Export Tool - {osm_url_basename}",
                    urn=self.get_new_urn()
                )
                run_node.global_urn = run_gurn
                run_node.action = 'run'
                run_node.node_type = 'osm-runner'
                run_node.input = None

                # Initialize properties
                for n in [concat_node, down_node, run_node]:
                    n.output = None
                    if not hasattr(n, 'custom_properties') or n.custom_properties is None:
                        n.custom_properties = {}
                    n.custom_properties['osm'] = osm_url

                # 4. Splicing logic
                # Insert concat above download
                self.insert_parent(node, concat_node)
                # Insert runner above concat
                self.insert_parent(concat_node, run_node)
                
                # Manual Sibling Attachment: 
                # Since Runner is now parent of Concat, we just add Downloader to Runner's children.
                if not hasattr(run_node, 'children') or run_node.children is None:
                    run_node.children = []
                
                # Avoid duplicates if multiple nodes in a group share a runner
                if down_node not in run_node.children:
                    run_node.children.append(down_node)

                # 5. Set original parent of the runner to 'import'
                runner_parent = self.find_parent(run_node.urn)
                if runner_parent:
                    runner_parent.action = 'import'
                    runner_parent.input = None 
                    if not hasattr(runner_parent, 'custom_properties') or runner_parent.custom_properties is None:
                        runner_parent.custom_properties = {}
                    runner_parent.custom_properties['osm'] = osm_url

        self.log.debug("OSM Tree complete: Runner is now parent to both Downloader and Concatenator.")