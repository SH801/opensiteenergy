from dotenv import load_dotenv

# Ideally user has created own .env file. If not copy over template
if not isfile('.env'): 
    print("Default .env file not found, creating it from template")
    shutil.copy('.env-template', '.env')

load_dotenv()

WORKING_FOLDER = str(Path(__file__).absolute().parent) + '/'

BUILD_FOLDER                        = WORKING_FOLDER + 'build-cli/'
QGIS_PYTHON_PATH                    = '/usr/bin/python3'
CKAN_URL                            = 'https://data.openwind.energy'
TILESERVER_URL                      = 'http://localhost:8080'
SERVER_BUILD                        = False

# Allow certain variables to be changed using environment variables

if os.environ.get("SERVER_BUILD") is not None: SERVER_BUILD = True
if os.environ.get("BUILD_FOLDER") is not None: BUILD_FOLDER = os.environ.get('BUILD_FOLDER')
if os.environ.get("QGIS_PYTHON_PATH") is not None: QGIS_PYTHON_PATH = os.environ.get('QGIS_PYTHON_PATH')
if os.environ.get("CKAN_URL") is not None: CKAN_URL = os.environ.get('CKAN_URL')
if os.environ.get("TILESERVER_URL") is not None: TILESERVER_URL = os.environ.get('TILESERVER_URL')

OPENWINDENERGY_VERSION              = "1.0"
TEMP_FOLDER                         = "temp/"
USE_MULTIPROCESSING                 = True
if SERVER_BUILD: USE_MULTIPROCESSING = True
if BUILD_FOLDER == "build-docker/": USE_MULTIPROCESSING = False
DEFAULT_HEIGHT_TO_TIP               = 124.2     # Based on openwind's own manual data on all large (>=75 m to tip-height) failed and successful UK onshore wind projects
DEFAULT_BLADE_RADIUS                = 47.8      # Based on openwind's own manual data on all large (>=75 m to tip-height) failed and successful UK onshore wind projects
HEIGHT_TO_TIP                       = DEFAULT_HEIGHT_TO_TIP
BLADE_RADIUS                        = DEFAULT_BLADE_RADIUS
CUSTOM_CONFIGURATION                = None
CUSTOM_CONFIGURATION_FOLDER         = BUILD_FOLDER + 'configuration/'
CUSTOM_CONFIGURATION_TABLE_PREFIX   = '__'
CUSTOM_CONFIGURATION_FILE_PREFIX    = 'custom--'
LATEST_OUTPUT_FILE_PREFIX           = 'latest--'
OSM_MAIN_DOWNLOAD                   = 'https://download.geofabrik.de/europe/united-kingdom-latest.osm.pbf'
OSM_CONFIG_FOLDER                   = BUILD_FOLDER + 'osm-export-yml/'
OSM_DOWNLOADS_FOLDER                = BUILD_FOLDER + 'osm-downloads/'
OSM_EXPORT_DATA                     = 'osm-export'
OSM_BOUNDARIES                      = 'osm-boundaries'
OSM_BOUNDARIES_YML                  = WORKING_FOLDER + OSM_BOUNDARIES + '.yml'
DATASETS_DOWNLOADS_FOLDER           = BUILD_FOLDER + 'datasets-downloads/'
OSM_LOOKUP                          = BUILD_FOLDER + 'datasets-osm.json'
STRUCTURE_LOOKUP                    = BUILD_FOLDER + 'datasets-structure.json'
BUFFER_LOOKUP                       = BUILD_FOLDER + 'datasets-buffers.json'
STYLE_LOOKUP                        = BUILD_FOLDER + 'datasets-style.json'
MAPAPP_FOLDER                       = BUILD_FOLDER + 'app/'
MAPAPP_JS_STRUCTURE                 = MAPAPP_FOLDER + 'datasets-latest-style.js'
MAPAPP_JS_BOUNDS_CENTER             = MAPAPP_FOLDER + 'bounds-centre.js'
MAPAPP_MAXBOUNDS                    = [[-49.262695,38.548165], [39.990234,64.848937]]
MAPAPP_FITBOUNDS                    = None
MAPAPP_CENTER                       = [-6, 55.273]
TILESERVER_FONTS_GITHUB             = 'https://github.com/openmaptiles/fonts'
TILESERVER_SRC_FOLDER               = WORKING_FOLDER + 'tileserver/'
TILESERVER_FOLDER                   = BUILD_FOLDER + 'tileserver/'
TILESERVER_DATA_FOLDER              = TILESERVER_FOLDER + 'data/'
TILESERVER_STYLES_FOLDER            = TILESERVER_FOLDER + 'styles/'
TILEMAKER_DOWNLOAD_SCRIPT           = TILESERVER_SRC_FOLDER + 'get-coastline-landcover.sh'
TILEMAKER_COASTLINE                 = WORKING_FOLDER + 'coastline/'
TILEMAKER_LANDCOVER                 = WORKING_FOLDER + 'landcover/'
TILEMAKER_COASTLINE_CONFIG          = TILESERVER_SRC_FOLDER + 'config-coastline.json'
TILEMAKER_COASTLINE_PROCESS         = TILESERVER_SRC_FOLDER + 'process-coastline.lua'
TILEMAKER_OMT_CONFIG                = TILESERVER_SRC_FOLDER + 'config-openmaptiles.json'
TILEMAKER_OMT_PROCESS               = TILESERVER_SRC_FOLDER + 'process-openmaptiles.lua'
QGIS_OUTPUT_FILE                    = BUILD_FOLDER + "windconstraints--latest.qgs"
FINALLAYERS_OUTPUT_FOLDER           = BUILD_FOLDER + 'output/'
FINALLAYERS_CONSOLIDATED            = 'windconstraints'
PERFORM_DOWNLOAD                    = True
REGENERATE_INPUT                    = False
REGENERATE_OUTPUT                   = False
OVERALL_CLIPPING_FILE               = 'overall-clipping.gpkg'
WORKING_CRS                         = 'EPSG:4326'
POSTGRES_HOST                       = os.environ.get("POSTGRES_HOST")
POSTGRES_DB                         = os.environ.get("POSTGRES_DB")
POSTGRES_USER                       = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD                   = os.environ.get("POSTGRES_PASSWORD")
DEBUG_RUN                           = False
OPENMAPTILES_HOSTED_FONTS           = "https://cdn.jsdelivr.net/gh/open-wind/openmaptiles-fonts/fonts/{fontstack}/{range}.pbf"
SKIP_FONTS_INSTALLATION             = False
CKAN_USER_AGENT                     = 'ckanapi/1.0 (+https://openwind.energy)'
DOWNLOAD_USER_AGENT                 = 'openwindenergy/' + OPENWINDENERGY_VERSION
LOG_SINGLE_PASS                     = WORKING_FOLDER + 'log.txt'
PROCESSING_START                    = None
