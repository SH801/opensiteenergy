def osmDownloadData():
    """
    Downloads core OSM data
    """

    global  BUILD_FOLDER, OSM_MAIN_DOWNLOAD, OSM_RELATED_FORMATS_FOLDER, TILEMAKER_DOWNLOAD_SCRIPT, TILEMAKER_COASTLINE, TILEMAKER_LANDCOVER, TILEMAKER_COASTLINE_CONFIG

    makeFolder(BUILD_FOLDER)
    makeFolder(OSM_RELATED_FORMATS_FOLDER)

    if not isfile(OSM_RELATED_FORMATS_FOLDER + basename(OSM_MAIN_DOWNLOAD)):

        LogMessage("Downloading latest OSM data")

        # Download to temp file in case download interrupted for any reason, eg. user clicks 'Stop processing'

        download_temp = OSM_RELATED_FORMATS_FOLDER + 'temp.pbf'
        if isfile(download_temp): os.remove(download_temp)

        runSubprocess(["wget", OSM_MAIN_DOWNLOAD, "-O", download_temp])

        shutil.copy(download_temp, OSM_RELATED_FORMATS_FOLDER + basename(OSM_MAIN_DOWNLOAD))
        if isfile(download_temp): os.remove(download_temp)

    LogMessage("Checking all files required for OSM tilemaker...")

    shp_extensions = ['shp', 'shx', 'dbf', 'prj']
    tilemaker_config_json = getJSON(TILEMAKER_COASTLINE_CONFIG)
    tilemaker_config_layers = list(tilemaker_config_json['layers'].keys())

    all_tilemaker_layers_downloaded = True
    for layer in tilemaker_config_layers:
        layer_elements = tilemaker_config_json['layers'][layer]
        if 'source' in layer_elements:
            for shp_extension in shp_extensions:
                source_file = layer_elements['source'].replace('.shp', '.' + shp_extension)
                if not isfile(source_file):
                    LogMessage("Missing file for OSM tilemaker: " + source_file)
                    all_tilemaker_layers_downloaded = False

    if all_tilemaker_layers_downloaded:
        LogMessage("All files downloaded for OSM tilemaker")
    else:
        LogMessage("Downloading global water and coastline data for OSM tilemaker")
        runSubprocess([TILEMAKER_DOWNLOAD_SCRIPT])



def getCountryFromArea(area):
    """
    Determine country that area is in using OSM_BOUNDARIES_GPKG
    """

    global OSM_BOUNDARIES
    global POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, WORKING_CRS
    global OSM_NAME_CONVERT

    osm_boundaries_table = reformatTableNameAbsolute(OSM_BOUNDARIES)
    countries = [OSM_NAME_CONVERT[country] for country in OSM_NAME_CONVERT.keys()]

    results = postgisGetResults("""
    WITH primaryarea AS
    (
        SELECT geom FROM %s WHERE (name = %s) OR (council_name = %s) LIMIT 1
    )
    SELECT 
        name, 
        ST_Area(ST_Intersection(primaryarea.geom, secondaryarea.geom)) geom_intersection 
    FROM %s secondaryarea, primaryarea 
    WHERE name = ANY (%s) AND ST_Intersects(primaryarea.geom, secondaryarea.geom) ORDER BY geom_intersection DESC LIMIT 1;
    """, (AsIs(osm_boundaries_table) , area, area, AsIs(osm_boundaries_table), countries, ))

    containing_country = results[0][0]

    for canonical_country in OSM_NAME_CONVERT.keys():
        if OSM_NAME_CONVERT[canonical_country] == containing_country: return canonical_country

    return None

