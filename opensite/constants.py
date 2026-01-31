import os
import logging
from pathlib import Path

class OpenSiteConstants:
    """Arbitrary application constants that don't change per environment."""

    # Directory script is run from
    WORKING_FOLDER              = str(Path(__file__).absolute().parent) + '/'

    # Redirect ogr2ogr warnings to log file
    os.environ['CPL_LOG']       = WORKING_FOLDER + 'log-ogr2ogr.txt'

    PROCESSING_STATE_FILE       = 'PROCESSING'
    PROCESSING_COMPLETE_FILE    = 'PROCESSINGCOMPLETE'

    # Default logging level for entire application
    LOGGING_LEVEL               = logging.DEBUG

    # How many seconds to update console
    DOWNLOAD_INTERVAL_TIME      = 10

    # Default CRS for spatial operations
    # Use EPSG:25830 for maximum precision across United Kingdom
    CRS_DEFAULT                 = 'EPSG:25830'

    # GeoJSON default CRS
    CRS_GEOJSON                 = 'EPSG:4326'

    # CRS of all exported GIS files
    CRS_OUTPUT                  = 'EPSG:4326'

    # Format text used by CKAN to indicate osm-export-tool YML file
    OSM_YML_FORMAT              = "osm-export-tool YML"

    # Format text used by CKAN to indicate Open Site Energy YML file
    SITES_YML_FORMAT            = "Open Site Energy YML"

    # CKAN formats we can accept
    CKAN_FORMATS                = \
                                [
                                    'GPKG', 
                                    'ArcGIS GeoServices REST API', 
                                    'GeoJSON', 
                                    'WFS', 
                                    'KML',
                                    OSM_YML_FORMAT, 
                                    SITES_YML_FORMAT, 
                                ]

    # CKAN formats we can download using default downloader
    CKAN_DEFAULT_DOWNLOADER     = \
                                [
                                    'OSM',
                                    'GPKG',
                                    'GeoJSON',
                                    OSM_YML_FORMAT, 
                                    SITES_YML_FORMAT, 
                                ]

    # File extensions we should expect from downloading these different CKAN formats
    CKAN_FILE_EXTENSIONS        = \
                                {
                                    'GPKG' : 'gpkg', 
                                    'ArcGIS GeoServices REST API': 'geojson', 
                                    'GeoJSON': 'geojson', 
                                    'WFS': 'gpkg', 
                                    'KML': 'geojson',
                                    OSM_YML_FORMAT: 'yml', 
                                    SITES_YML_FORMAT: 'yml', 
                                }

    # Priority of downloads
    DOWNLOADS_PRIORITY          = \
                                [
                                    'OSM',
                                    SITES_YML_FORMAT,
                                    OSM_YML_FORMAT,
                                ]
    
    # Formats to always download - typically small and may be subject to regular change
    ALWAYS_DOWNLOAD             = \
                                [
                                    SITES_YML_FORMAT,
                                    OSM_YML_FORMAT,
                                ]

    # OSM-related formats - so they all go in same folder
    OSM_DOWNLOADS               = \
                                [
                                    'OSM',
                                    OSM_YML_FORMAT,
                                ]

    # Location of clipping master file
    CLIPPING_MASTER             = 'clipping-master-' + CRS_DEFAULT.replace(':', '-') + '.gpkg'

    # Root build directory
    BUILD_ROOT                  = Path(os.getenv("BUILD_FOLDER", "build"))
    
    # Sub-directories
    DOWNLOAD_FOLDER             = BUILD_ROOT / "downloads"
    CACHE_FOLDER                = BUILD_ROOT / "cache"
    LOG_FOLDER                  = BUILD_ROOT / "logs"
    OSM_FOLDER                  = DOWNLOAD_FOLDER / "osm"
    OUTPUT_FOLDER               = BUILD_ROOT / "output"
    OUTPUT_LAYERS_FOLDER        = OUTPUT_FOLDER / "layers"

    ALL_FOLDERS                 = \
                                [
                                    LOG_FOLDER,
                                    OSM_FOLDER,
                                    CACHE_FOLDER,
                                    BUILD_ROOT,
                                    DOWNLOAD_FOLDER,
                                    OUTPUT_FOLDER,
                                    OUTPUT_LAYERS_FOLDER,
                                ]
    
    # Acceptable CLI properties
    TREE_BRANCH_PROPERTIES      = \
                                {
                                    'functions':    [
                                                        'height-to-tip', 
                                                        'blade-radius'
                                                    ],
                                    'default':      [
                                                        'title', 
                                                        'type', 
                                                        'clipping-path', 
                                                        'osm',
                                                        'ckan',
                                                    ]
                                }

    # Processing grid is used to cut up core datasets into grid squares
    # to reduce memory load on ST_Union. All final layers will have ST_Union
    # so it's okay to cut up early datasets before this
    GRID_PROCESSING_SPACING     = 100 * 1000 # Size of grid squares in metres, ie. 500km

    # Output grid is used to cut up final output into grid squares 
    # in order to improve quality and performance of rendering 
    GRID_OUTPUT_SPACING         = 100 * 1000 # Size of grid squares in metres, ie. 100km

    # Basename of OSM boundaries files
    # If [basename].gpkg file doesn't exist, processing nodes will be added to create it
    OSM_BOUNDARIES              = 'osm-boundaries'

    # Location of OSM boundaries osm-export-tool YML file
    OSM_BOUNDARIES_YML          = OSM_BOUNDARIES + '.yml'

    # Database tables
    DATABASE_GENERAL_PREFIX     = 'opensite_'
    DATABASE_BASE               = '_' + DATABASE_GENERAL_PREFIX
    OPENSITE_REGISTRY           = DATABASE_BASE + 'registry'
    OPENSITE_BRANCH             = DATABASE_BASE + 'branch'
    OPENSITE_CLIPPINGMASTER     = DATABASE_BASE + 'clipping_master'
    OPENSITE_CLIPPINGTEMP       = DATABASE_BASE + 'clipping_temp'
    OPENSITE_GRIDPROCESSING     = DATABASE_BASE + 'grid_processing'
    OPENSITE_GRIDOUTPUT         = DATABASE_BASE + 'grid_output'
    OPENSITE_OSMBOUNDARIES      = DATABASE_BASE + OSM_BOUNDARIES.replace('-', '_')

    # Lookup to convert internal areas to OSM names
    OSM_NAME_CONVERT            = \
                                {
                                    'england': 'England',
                                    'wales': 'Cymru / Wales',
                                    'Wales': 'Cymru / Wales',
                                    'scotland': 'Alba / Scotland',
                                    'Scotland': 'Alba / Scotland',
                                    'northern-ireland': 'Northern Ireland / Tuaisceart Éireann',
                                    'Northern Ireland': 'Northern Ireland / Tuaisceart Éireann'
                                }
    
    # Colour codes for logger
    LOGGER_RED                  = "\033[31m"
    LOGGER_GREEN                = "\033[32m"
    LOGGER_YELLOW               = "\033[33m"
    LOGGER_BLUE                 = "\033[34m"
    LOGGER_RESET                = "\033[0m"


