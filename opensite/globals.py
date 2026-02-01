
BUILD_FOLDER                        = WORKING_FOLDER + 'build-cli/'
QGIS_PYTHON_PATH                    = '/usr/bin/python3'
CKAN_URL                            = 'https://data.openwind.energy'
TILESERVER_URL                      = 'http://localhost:8080'
SERVER_BUILD                        = False

# Allow certain variables to be changed using environment variables

if os.environ.get("SERVER_BUILD") is not None: SERVER_BUILD = True
if os.environ.get("QGIS_PYTHON_PATH") is not None: QGIS_PYTHON_PATH = os.environ.get('QGIS_PYTHON_PATH')
if os.environ.get("TILESERVER_URL") is not None: TILESERVER_URL = os.environ.get('TILESERVER_URL')

TEMP_FOLDER                         = "temp/"
USE_MULTIPROCESSING                 = True
if SERVER_BUILD: USE_MULTIPROCESSING = True
if BUILD_FOLDER == "build-docker/": USE_MULTIPROCESSING = False
MAPAPP_FOLDER                       = BUILD_FOLDER + 'app/'
MAPAPP_JS_STRUCTURE                 = MAPAPP_FOLDER + 'datasets-latest-style.js'
MAPAPP_JS_BOUNDS_CENTER             = MAPAPP_FOLDER + 'bounds-centre.js'
MAPAPP_MAXBOUNDS                    = [[-49.262695,38.548165], [39.990234,64.848937]]
MAPAPP_FITBOUNDS                    = None
MAPAPP_CENTER                       = [-6, 55.273]
TILESERVER_SRC_FOLDER               = WORKING_FOLDER + 'tileserver/'
TILESERVER_FOLDER                   = BUILD_FOLDER + 'tileserver/'
QGIS_OUTPUT_FILE                    = BUILD_FOLDER + "windconstraints--latest.qgs"
FINALLAYERS_OUTPUT_FOLDER           = BUILD_FOLDER + 'output/'
FINALLAYERS_CONSOLIDATED            = 'windconstraints'
OPENMAPTILES_HOSTED_FONTS           = "https://cdn.jsdelivr.net/gh/open-wind/openmaptiles-fonts/fonts/{fontstack}/{range}.pbf"
SKIP_FONTS_INSTALLATION             = False
DOWNLOAD_USER_AGENT                 = 'openwindenergy/' + OPENWINDENERGY_VERSION
LOG_SINGLE_PASS                     = WORKING_FOLDER + 'log.txt'
PROCESSING_START                    = None
