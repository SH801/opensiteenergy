
def buildQGISFile():
    """
    Builds QGIS file
    """

    # Uses separate process to allow use of QGIS-specific Python

    global QGIS_PYTHON_PATH, QGIS_OUTPUT_FILE

    LogMessage("Attempting to generate QGIS file...")

    if not isfile(QGIS_PYTHON_PATH):

        LogMessage(" --> Unable to locate QGIS Python at: " + QGIS_PYTHON_PATH)
        LogMessage(" --> Edit your .env file to include the full path to QGIS's Python and rerun")
        LogMessage(" --> *** SKIPPING QGIS FILE CREATION ***")

    else:

        runSubprocessAndOutput([QGIS_PYTHON_PATH, 'build-qgis.py', QGIS_OUTPUT_FILE])

