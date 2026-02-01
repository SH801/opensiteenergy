def deleteDatasetFiles(dataset):
    """
    Deletes all files specifically relating to dataset
    """

    global CUSTOM_CONFIGURATION, CUSTOM_CONFIGURATION_FILE_PREFIX
    global HEIGHT_TO_TIP, DATASETS_DOWNLOADS_FOLDER, FINALLAYERS_OUTPUT_FOLDER, TILESERVER_DATA_FOLDER

    possible_extensions = ['geojson', 'gpkg', 'shp', 'shx', 'dbf', 'prj', 'sld', 'mbtiles']

    custom_configuration_prefix = ''
    if CUSTOM_CONFIGURATION is not None: custom_configuration_prefix = CUSTOM_CONFIGURATION_FILE_PREFIX

    table = reformatTableName(dataset)
    turbine_parameters_file_prefix = buildTurbineParametersPrefix().replace('_', '-')
    for possible_extension in possible_extensions:
        dataset_basename = dataset + '.' + possible_extension
        latest_basename = getFinalLayerLatestName(table) + '.' + possible_extension
        possible_files = []
        possible_files.append(DATASETS_DOWNLOADS_FOLDER + dataset_basename)
        possible_files.append(FINALLAYERS_OUTPUT_FOLDER + latest_basename)
        possible_files.append(FINALLAYERS_OUTPUT_FOLDER + custom_configuration_prefix + 'tip-any--' + dataset_basename)
        possible_files.append(FINALLAYERS_OUTPUT_FOLDER + custom_configuration_prefix + turbine_parameters_file_prefix + dataset_basename)
        possible_files.append(TILESERVER_DATA_FOLDER + latest_basename)

        for possible_file in possible_files:
            if isfile(possible_file): 
                LogMessage(" --> Deleting: " + possible_file)
                os.remove(possible_file)

def deleteDatasetTables(dataset, all_tables):
    """
    Deletes all tables specifically relating to dataset
    """

    table = reformatTableName(dataset)
    buffer = getDatasetBuffer(dataset)

    possible_tables = []
    possible_tables.append(table)
    possible_tables.append(buildProcessedTableName(table))
    possible_tables.append(buildFinalLayerTableName(table))
    if buffer is not None:
        bufferedTable = buildBufferTableName(table, buffer)
        possible_tables.append(bufferedTable)
        possible_tables.append(buildProcessedTableName(bufferedTable))

    # We update internal array of all_tables to minimise load on PostGIS
    for possible_table in possible_tables:
        if possible_table in all_tables:
            LogMessage(" --> Dropping PostGIS table: " + possible_table)
            postgisDropTable(possible_table)
            all_tables.remove(possible_table)

    return all_tables

def deleteAncestors(dataset, all_tables=None):
    """
    Deletes parent/ancestor files and parent/ancestor tables derived from dataset
    """

    dataset = dataset.split('.')[0]

    if all_tables is None: all_tables = postgisGetAllTables()

    LogMessage("Deleting files and tables derived from: " + dataset)

    dataset = reformatDatasetName(dataset)
    core_dataset = getCoreDatasetName(dataset)
    ancestors = getAllAncestors(core_dataset, include_initial_dataset=False)

    for ancestor in ancestors:
        deleteDatasetFiles(ancestor)
        all_tables = deleteDatasetTables(ancestor, all_tables)

    return all_tables

def deleteDatasetAndAncestors(dataset, all_tables=None):
    """
    Deletes specific dataset by deleting all files and tables specifically associated 
    with dataset and all parent/ancestor files and parent/ancestor tables derived from dataset
    """

    dataset = dataset.split('.')[0]

    if all_tables is None: all_tables = postgisGetAllTables()

    LogMessage("Deleting files and tables derived from: " + dataset)

    dataset = reformatDatasetName(dataset)
    core_dataset = getCoreDatasetName(dataset)
    ancestors = getAllAncestors(core_dataset)

    for ancestor in ancestors:
        deleteDatasetFiles(ancestor)
        all_tables = deleteDatasetTables(ancestor, all_tables)

    return all_tables


def purgeAll():
    """
    Deletes all database tables and build folder
    """

    global WORKING_FOLDER, BUILD_FOLDER, TILESERVER_FOLDER, OSM_RELATED_FORMATS_FOLDER, OSM_EXPORT_DATA, OSM_CONFIG_FOLDER, DATASETS_DOWNLOADS_FOLDER

    postgisDropAllTables()

    tileserver_folder_name = basename(TILESERVER_FOLDER[:-1])
    build_files = getFilesInFolder(BUILD_FOLDER)
    for build_file in build_files: 
        # Don't delete log files from BUILD_FOLDER
        if not build_file.endswith('.log'): os.remove(BUILD_FOLDER + build_file)
    osm_files = getFilesInFolder(OSM_RELATED_FORMATS_FOLDER)
    for osm_file in osm_files: os.remove(OSM_RELATED_FORMATS_FOLDER + osm_file)
    tileserver_files = getFilesInFolder(TILESERVER_FOLDER)
    for tileserver_file in tileserver_files: os.remove(TILESERVER_FOLDER + tileserver_file)

    pwd = os.path.dirname(os.path.realpath(__file__))

    # Delete items in BUILD_FOLDER

    subfolders = [ f.path for f in os.scandir(BUILD_FOLDER) if f.is_dir() ]
    absolute_build_folder = os.path.abspath(BUILD_FOLDER)

    for subfolder in subfolders:

        # Don't delete 'postgres' folder as managed by separate docker instance
        # Don't delete 'tileserver' folder yet as some elements are managed separately 
        # Don't delete 'landcover' and 'coastline' folders as managed by docker compose 
        if basename(subfolder) in ['postgres', tileserver_folder_name, 'coastline', 'landcover']: continue

        subfolder_absolute = os.path.abspath(subfolder)

        if len(subfolder_absolute) < len(absolute_build_folder) or not subfolder_absolute.startswith(absolute_build_folder):
            LogFatalError("Attempting to delete folder outside build folder, aborting")

        shutil.rmtree(subfolder_absolute)

    # Delete all items in 'landcover' and 'coastline' folders but keep folders in case managed by docker

    deleteFolderContentsKeepFolder(WORKING_FOLDER + 'coastline/')
    deleteFolderContentsKeepFolder(WORKING_FOLDER + 'landcover/')

    # Delete selected items in TILESERVER_FOLDER

    subfolders = [ f.path for f in os.scandir(TILESERVER_FOLDER) if f.is_dir() ]
    absolute_tileserver_folder = os.path.abspath(TILESERVER_FOLDER)

    for subfolder in subfolders:

        # Don't delete 'fonts' as this is created by openwindenergy-fonts
        if basename(subfolder) in ['fonts']: continue

        subfolder_absolute = os.path.abspath(subfolder)

        if len(subfolder_absolute) < len(absolute_tileserver_folder) or not subfolder_absolute.startswith(absolute_tileserver_folder):
            LogFatalError("Attempting to delete folder outside tileserver folder, aborting")

        shutil.rmtree(subfolder_absolute)