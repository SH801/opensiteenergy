import json
import logging
import os
import subprocess
from psycopg2 import pool, sql, Error
from opensite.constants import OpenSiteConstants
from opensite.postgis.base import PostGISBase
from opensite.logging.opensite import OpenSiteLogger

class OpenSitePostGIS(PostGISBase):

    OPENSITE_REGISTRY       = OpenSiteConstants.OPENSITE_REGISTRY
    OPENSITE_BRANCH         = OpenSiteConstants.OPENSITE_BRANCH
    OPENSITE_CLIPPINGMASTER = OpenSiteConstants.OPENSITE_CLIPPINGMASTER
    OPENSITE_GRIDPROCESSING = OpenSiteConstants.OPENSITE_GRIDPROCESSING
    OPENSITE_GRIDBUFFEDGES  = OpenSiteConstants.OPENSITE_GRIDBUFFEDGES
    OPENSITE_GRIDOUTPUT     = OpenSiteConstants.OPENSITE_GRIDOUTPUT
    OPENSITE_OSMBOUNDARIES  = OpenSiteConstants.OPENSITE_OSMBOUNDARIES
    
    def __init__(self, log_level=logging.INFO):
        super().__init__(log_level)
        self.log = OpenSiteLogger("OpenSitePostGIS", log_level)
        self._ensure_registry_exists()

    def purge_database(self):
        """Drops all tables with the opensite prefix (both internal and data tables)."""
        # Matches _opensite_branch, _opensite_registry, and opensite_hash...
        sql_find = f"""
            SELECT table_name , table_schema
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND (table_name LIKE '{OpenSiteConstants.DATABASE_GENERAL_PREFIX.replace('_', r'\_')}%' OR table_name LIKE '{OpenSiteConstants.DATABASE_BASE.replace('_', r'\_')}%')
            AND table_type = 'BASE TABLE';
        """
        
        tables_to_drop = self.fetch_all(sql_find)
        if not tables_to_drop:
            self.log.info("No OpenSite tables found to purge.")
            return

        self.log.warning(f"Purging {len(tables_to_drop)} tables from the database...")
        
        for row in tables_to_drop:
            table = row['table_name']
            try:
                # CASCADE handles foreign keys or views that might depend on these tables
                self.execute_query(f"DROP TABLE IF EXISTS {table} CASCADE;")

                if hasattr(self, 'connection'):
                    self.connection.commit()
                    self.log.info("Transaction committed successfully.")
                    self.log.debug(f"Dropped: {table}")

            except Exception as e:
                self.log.error(f"Failed to drop {table}: {e}")
        
        self.log.info("Database purge complete.")

    def _ensure_registry_exists(self):
        """Creates the master lookup table if it doesn't exist."""

        self.log.debug(f"Creating {self.OPENSITE_BRANCH} table")

        # Audit table for branch configuration state
        self.execute_query(f"""
        CREATE TABLE IF NOT EXISTS {self.OPENSITE_BRANCH} (
            yml_hash TEXT PRIMARY KEY,
            branch_name TEXT NOT NULL,
            config_json JSONB NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        self.log.debug(f"Creating {self.OPENSITE_REGISTRY} table")

        # Human-readable lookup for every node
        self.execute_query(f"""
        CREATE TABLE IF NOT EXISTS {self.OPENSITE_REGISTRY} (
            completed BOOLEAN DEFAULT FALSE,
            table_id TEXT PRIMARY KEY,
            human_name TEXT NOT NULL,
            branch_name TEXT NOT NULL,
            yml_hash TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

    def sync_registry(self):
        """
        Synchronizes registry, physical tables, and branch metadata.
        """
        self.log.info("Starting registry synchronization...")
        
        # 1. Get current registry state
        registry_entries = self.fetch_all(f"SELECT table_id, completed FROM {self.OPENSITE_REGISTRY}")
        registry_names = {row['table_id'] for row in registry_entries}
        
        # 2. Get physical tables
        protected_tables = {
            self.OPENSITE_REGISTRY, 
            self.OPENSITE_BRANCH, 
            self.OPENSITE_CLIPPINGMASTER,
            self.OPENSITE_GRIDPROCESSING,
            self.OPENSITE_GRIDBUFFEDGES,
            self.OPENSITE_GRIDOUTPUT,
            self.OPENSITE_OSMBOUNDARIES,
            'spatial_ref_sys', 
            'geography_columns', 
            'geometry_columns', 
            'raster_columns', 
            'raster_overview'
        }
        physical_tables = {t for t in self.get_table_names() if t not in protected_tables}

        # --- Step A & B: Clean the Registry ---
        for entry in registry_entries:
            table_id = entry['table_id']
            completed = entry.get('completed')

            if not completed:
                self.log.debug(f"Removing incomplete registry entry: {table_id}")
                self.execute_query(f"DELETE FROM {self.OPENSITE_REGISTRY} WHERE table_id = %s", (table_id,))
                registry_names.discard(table_id)
                continue

            if table_id not in physical_tables:
                self.log.debug(f"Removing orphaned registry entry (no table found): {table_id}")
                self.execute_query(f"DELETE FROM {self.OPENSITE_REGISTRY} WHERE table_id = %s", (table_id,))
                registry_names.discard(table_id)

        # --- Step C: Clean the Database (Untracked Tables) ---
        for table_id in physical_tables:
            if table_id not in registry_names :
                self.log.warning(f"Dropping untracked table: {table_id}")
                self.execute_query(f'DROP TABLE IF EXISTS "{table_id}" CASCADE')

        # --- Step D: Clean the Branches ---
        # We look for branch_name in {self.OPENSITE_BRANCH} that no longer has 
        # ANY associated records in {self.OPENSITE_REGISTRY}
        self.log.info("Checking for orphaned branches...")
        
        orphaned_branches_sql = f"""
            SELECT b.branch_name 
            FROM {self.OPENSITE_BRANCH} b
            LEFT JOIN {self.OPENSITE_REGISTRY} r ON b.branch_name = r.branch_name
            WHERE r.branch_name IS NULL
        """
        orphaned_branches = self.fetch_all(orphaned_branches_sql)
        
        for branch in orphaned_branches:
            b_name = branch['branch_name']
            self.log.warning(f"Removing orphaned branch metadata: {b_name}")
            self.execute_query(f"DELETE FROM {self.OPENSITE_BRANCH} WHERE branch_name = %s", (b_name,))

        self.log.info("Registry and branch synchronization complete.")

    def register_branch(self, branch_name, yml_hash, config_dict):
        """Stores the full configuration JSON for a specific hash."""

        self.log.debug(f"Registering branch in {self.OPENSITE_BRANCH} {yml_hash} {branch_name}")

        query = f"""
            INSERT INTO {self.OPENSITE_BRANCH} (yml_hash, branch_name, config_json)
            VALUES (%s, %s, %s)
            ON CONFLICT (yml_hash) DO UPDATE SET
                config_json = EXCLUDED.config_json;
        """
        self.execute_query(query, (yml_hash, branch_name, json.dumps(config_dict)))

    def register_node(self, node, branch=None, override_branch_name=None):
        """
        Inserts a node's table mapping into the registry.
        Expects node.output and branch.custom_properties['hash'] to exist.
        """
        output = getattr(node, 'output', None)
        human_name = node.name
        branch_name, yml_hash = '', ''
        if branch: 
            branch_name = branch.name
            yml_hash = branch.custom_properties.get('hash')
        if override_branch_name:
            branch_name = override_branch_name

        if output:
            self.log.debug(f"Registering node in opensite_registery {output} {human_name} {branch_name}")

            query = f"""
            INSERT INTO {self.OPENSITE_REGISTRY} (table_id, human_name, branch_name, yml_hash)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (table_id) DO UPDATE SET
                human_name = EXCLUDED.human_name,
                branch_name = EXCLUDED.branch_name;
            """
            self.execute_query(query, (output, human_name, branch_name, yml_hash))

    def set_table_completed(self, table_id):
        """
        Updates an existing node's status. 
        Returns True if a row was updated, False if the URN was missing.
        """
        sql = f"""
            UPDATE {self.OPENSITE_REGISTRY} 
            SET completed = true, 
                updated_at = CURRENT_TIMESTAMP
            WHERE table_id = %s;
        """
        conn = self.pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, (table_id, ))
                updated_rows = cursor.rowcount
                conn.commit()
                return updated_rows > 0
            
        except (Exception, Error) as e:
            # This will catch syntax errors, connection issues, or constraint violations
            self.log.error(f"Failed to update registry for {table_id}: {e}")
            if conn:
                conn.rollback() # Important: roll back the failed transaction
            return False

        finally:
            self.pool.putconn(conn)

    def import_spatial_data(self, spatial_data_file, spatial_data_table):
        """
        Generic import function for standardised input spatial data files
        To save time, we assume:
        - CRS of source file is OpenSiteConstants.CRS_DEFAULT
        - Geometry type doesn't need changing
        - There are absolutely no errors
        These assumptions do not hold for OpenSiteImporter in opensite.processing.importer.py
        """

        # Base ogr2ogr Command
        cmd = [
            "ogr2ogr",
            "-f", "PostgreSQL",
            self.get_ogr_connection_string(),
            spatial_data_file,
            "-overwrite",
            "-lco", "GEOMETRY_NAME=geom",
            "-nln", spatial_data_table,
            "-nlt", "PROMOTE_TO_MULTI",
            "--config", "PG_USE_COPY", "YES",
            "--config", "OGR_PG_ENABLE_METADATA", "NO"
        ]

        self.log.info(f"Importing file {os.path.basename(spatial_data_file)} to table '{spatial_data_table}'")

        try:
            # Execute shell command
            subprocess.run(cmd, capture_output=True, text=True, check=True)

            self.log.info(f"{os.path.basename(spatial_data_file)} imported to table '{spatial_data_table}'")
            return True
        
        except subprocess.CalledProcessError as e:
            self.log.error(f"PostGIS Import Error: {os.path.basename(spatial_data_file)} {e.stderr}")
            return False

    def export_spatial_data(self, spatial_data_table, spatial_data_layer_name, spatial_data_file):
        """
        Generic export function for standardised export of spatial data files
        """

        crs_output = OpenSiteConstants.CRS_OUTPUT

        # Base ogr2ogr Command
        cmd = [
            "ogr2ogr",
            spatial_data_file,
            self.get_ogr_connection_string(),
            "-overwrite",
            "-nln", spatial_data_layer_name,
            "-nlt", "POLYGON",
            "-dialect", "sqlite", 
            "-sql", 
            f"SELECT geom geometry FROM '{spatial_data_table}'",
            "-s_srs", OpenSiteConstants.CRS_DEFAULT,
            "-t_srs", OpenSiteConstants.CRS_OUTPUT
        ]

        self.log.info(f"Exporting table '{spatial_data_table}' to file {os.path.basename(spatial_data_file)}")

        try:
            # Execute shell command
            subprocess.run(cmd, capture_output=True, text=True, check=True)

            self.log.info(f"Table {spatial_data_table} exported to {os.path.basename(spatial_data_file)}")
            return True
        
        except subprocess.CalledProcessError as e:
            self.log.error(f"PostGIS Export Error: {spatial_data_table} {e.stderr}")
            return False
        
    def get_areas_bounds(self, areas, crs_input=OpenSiteConstants.CRS_DEFAULT, crs_output=OpenSiteConstants.CRS_OUTPUT):
        """
        Get collective bounds of geometries for a list of area names
        """

        # Normalize the list of names using your conversion map
        processed_areas = []
        for area in areas:
            if area in OpenSiteConstants.OSM_NAME_CONVERT:
                area = OpenSiteConstants.OSM_NAME_CONVERT[area]
            processed_areas.append(area)

        # Prepare parameters
        # We use a literal tuple/list for the SQL 'ANY' comparison
        dbparams = {
            "crs_input": sql.Literal(self.extract_crs_as_number(crs_input)),
            "crs_output": sql.Literal(self.extract_crs_as_number(crs_output)),
            'table': sql.Identifier(OpenSiteConstants.OPENSITE_OSMBOUNDARIES),
            'areas': sql.Literal(processed_areas)
        }

        # Use ILIKE ANY to match any string in the list
        query_maxbounds = sql.SQL("""
        SELECT 
            ST_XMin(extent_output_crs) AS left,
            ST_YMin(extent_output_crs) AS bottom,
            ST_XMax(extent_output_crs) AS right,
            ST_YMax(extent_output_crs) AS top
        FROM 
            (
            SELECT ST_Transform(ST_SetSRID(ST_Extent(geom), {crs_input}), {crs_output}) AS extent_output_crs 
            FROM {table} 
            WHERE name ILIKE ANY ({areas}) OR council_name ILIKE ANY ({areas})
            ) AS subquery
        """).format(**dbparams)

        try:
            results = self.fetch_all(query_maxbounds)
            
            # Check if we actually found anything
            if not results or results[0]['left'] is None:
                self.log.debug(f"Unable to find any clipping areas from list {areas} in boundary database")
                return None

            return results[0]
            
        except Exception as e:
            self.log.error(f"PostGIS error while fetching multi-area bounds: {e}")
            return None

    def get_country_from_area(self, area):
        """
        Determine country that single area is in using OPENSITE_OSMBOUNDARIES
        """

        # Get list of all possible OSM country names from OSM_NAME_CONVERT
        countries = [OpenSiteConstants.OSM_NAME_CONVERT[country] for country in OpenSiteConstants.OSM_NAME_CONVERT.keys()]

        dbparams = \
        {
            'area':         sql.Literal(area),
            'boundaries':   sql.Identifier(OpenSiteConstants.OPENSITE_OSMBOUNDARIES),
            'countries':    sql.Literal(countries),
        }

        query_find_containing_countries = sql.SQL("""
        WITH primaryarea AS
        (
            SELECT geom FROM {boundaries} WHERE (name ILIKE {area}) OR (council_name ILIKE {area}) LIMIT 1
        )
        SELECT 
            name, ST_Area(ST_Intersection(primaryarea.geom, secondaryarea.geom)) geom_intersection 
        FROM 
            {boundaries} secondaryarea, primaryarea 
        WHERE 
            name = ANY ({countries}) AND ST_Intersects(primaryarea.geom, secondaryarea.geom) 
        ORDER BY geom_intersection DESC LIMIT 1;
        """).format(**dbparams)
                
        containing_geometries = self.fetch_all(query_find_containing_countries)

        if len(containing_geometries) > 0:
            containing_country = containing_geometries[0]['name']
            for canonical_country in OpenSiteConstants.OSM_NAME_CONVERT.keys():
                if OpenSiteConstants.OSM_NAME_CONVERT[canonical_country] == containing_country: 
                    return OpenSiteConstants.OSM_NAME_CONVERT[canonical_country]

        return None
