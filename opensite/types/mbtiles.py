def buildTileserverFiles():
    """
    Builds files required for tileserver-gl
    """

    global  CUSTOM_CONFIGURATION, CUSTOM_CONFIGURATION_FILE_PREFIX, LATEST_OUTPUT_FILE_PREFIX, TEMP_FOLDER
    global  OVERALL_CLIPPING_FILE, TILESERVER_URL, TILESERVER_FONTS_GITHUB, TILESERVER_SRC_FOLDER, TILESERVER_FOLDER, TILESERVER_DATA_FOLDER, TILESERVER_STYLES_FOLDER, \
            OSM_DOWNLOADS_FOLDER, OSM_MAIN_DOWNLOAD, BUILD_FOLDER, FINALLAYERS_OUTPUT_FOLDER, FINALLAYERS_CONSOLIDATED, MAPAPP_FOLDER
    global  TILEMAKER_COASTLINE_CONFIG, TILEMAKER_COASTLINE_PROCESS, TILEMAKER_OMT_CONFIG, TILEMAKER_OMT_PROCESS, SKIP_FONTS_INSTALLATION, OPENMAPTILES_HOSTED_FONTS

    # Run tileserver build process

    LogMessage("Creating tileserver files")

    makeFolder(TILESERVER_FOLDER)
    makeFolder(TILESERVER_DATA_FOLDER)
    makeFolder(TILESERVER_STYLES_FOLDER)
    makeFolder(TEMP_FOLDER)

    # Legacy issue: housekeeping of final output and tileserver folders due to shortening of
    # specific dataset names leaving old files with old names that cause problems
    # Also general shortening of output filenames to allow for blade radius information

    legacy_delete_items = ['tipheight-', 'public-roads-a-and-b-roads-and-motorways', 'openwind.json']
    for legacy_delete_item in legacy_delete_items:
        for file_name in getFilesInFolder(FINALLAYERS_OUTPUT_FOLDER):
            if legacy_delete_item in file_name: os.remove(FINALLAYERS_OUTPUT_FOLDER + file_name)
        for file_name in getFilesInFolder(TILESERVER_DATA_FOLDER):
            if legacy_delete_item in file_name: os.remove(TILESERVER_DATA_FOLDER + file_name)
        for file_name in getFilesInFolder(TILESERVER_STYLES_FOLDER):
            if legacy_delete_item in file_name: os.remove(TILESERVER_STYLES_FOLDER + file_name)

    # Copy 'sprites' folder

    if not isdir(TILESERVER_FOLDER + 'sprites/'):
        shutil.copytree(TILESERVER_SRC_FOLDER + 'sprites/', TILESERVER_FOLDER + 'sprites/')

    # Copy index.html

    shutil.copy(TILESERVER_SRC_FOLDER + 'index.html', MAPAPP_FOLDER + 'index.html')

    # Modify 'openmaptiles.json' and export to tileserver folder

    openmaptiles_style_file_src = TILESERVER_SRC_FOLDER + 'openmaptiles.json'
    openmaptiles_style_file_dst = TILESERVER_STYLES_FOLDER + 'openmaptiles.json'
    openmaptiles_style_json = getJSON(openmaptiles_style_file_src)
    openmaptiles_style_json['sources']['openmaptiles']['url'] = TILESERVER_URL + '/data/openmaptiles.json'

    # Either use hosted version of fonts or install local fonts folder

    use_font_folder = False
    if SKIP_FONTS_INSTALLATION:
        fonts_url = OPENMAPTILES_HOSTED_FONTS
    else:
        if installTileserverFonts():
            use_font_folder = True
            fonts_url = TILESERVER_URL + '/fonts/{fontstack}/{range}.pbf'
        else:
            LogMessage("Attempt to build fonts failed, using hosted fonts instead")
            fonts_url = OPENMAPTILES_HOSTED_FONTS

    openmaptiles_style_json['glyphs'] = fonts_url

    with open(openmaptiles_style_file_dst, "w") as json_file: json.dump(openmaptiles_style_json, json_file, indent=4)

    attribution = "Source data copyright of multiple organisations. For all data sources, see <a href=\"" + CKAN_URL + "\" target=\"_blank\">" + CKAN_URL.replace('https://', '') + "</a>"
    openwind_style_file = TILESERVER_STYLES_FOLDER + 'openwindenergy.json'
    openwind_style_json = openmaptiles_style_json
    openwind_style_json['name'] = 'Open Wind Energy'
    openwind_style_json['id'] = 'openwindenergy'
    openwind_style_json['sources']['attribution']['attribution'] += " " + attribution

    basemap_mbtiles = TILESERVER_DATA_FOLDER + basename(OSM_MAIN_DOWNLOAD).replace(".osm.pbf", ".mbtiles")

    # Create basemap mbtiles

    if not isfile(basemap_mbtiles):

        osmDownloadData()

        LogMessage("Creating basemap: " + basename(basemap_mbtiles))

        LogMessage("Generating global coastline mbtiles...")

        bbox_entireworld = "-180,-85,180,85"
        bbox_unitedkingdom_padded = "-49.262695,38.548165,39.990234,64.848937"

        inputs = runSubprocess(["tilemaker", \
                                "--input", OSM_DOWNLOADS_FOLDER + basename(OSM_MAIN_DOWNLOAD), \
                                "--output", basemap_mbtiles, \
                                "--bbox", bbox_unitedkingdom_padded, \
                                "--process", TILEMAKER_COASTLINE_PROCESS, \
                                "--config", TILEMAKER_COASTLINE_CONFIG ])

        LogMessage("Merging " + basename(OSM_MAIN_DOWNLOAD) + " into global coastline mbtiles...")

        inputs = runSubprocess(["tilemaker", \
                                "--input", OSM_DOWNLOADS_FOLDER + basename(OSM_MAIN_DOWNLOAD), \
                                "--output", basemap_mbtiles, \
                                "--merge", \
                                "--process", TILEMAKER_OMT_PROCESS, \
                                "--config", TILEMAKER_OMT_CONFIG ])

        LogMessage("Basemap mbtiles created: " + basename(basemap_mbtiles))

    # Run tippecanoe regardless of whether existing mbtiles exist

    style_lookup = getStyleLookup()
    dataset_style_lookup = {}
    for style_item in style_lookup:
        dataset_id = style_item['dataset']
        dataset_style_lookup[dataset_id] = {'title': style_item['title'], 'color': style_item['color'], 'level': style_item['level'], 'defaultactive': style_item['defaultactive']}
        if 'children' in style_item:
            for child in style_item['children']:
                child_dataset_id = child['dataset']
                dataset_style_lookup[child_dataset_id] = {'title': child['title'], 'color': child['color'], 'level': child['level'], 'defaultactive': child['defaultactive']}

    # Get bounds of clipping area for use in tileserver-gl config file creation

    clipping_table = reformatTableName(OVERALL_CLIPPING_FILE)
    clipping_union_table = buildUnionTableName(clipping_table)
    clipping_bounds_dict = postgisGetTableBounds(clipping_union_table)
    clipping_bounds = [clipping_bounds_dict['left'], clipping_bounds_dict['bottom'], clipping_bounds_dict['right'], clipping_bounds_dict['top']]

    output_files = getFilesInFolder(FINALLAYERS_OUTPUT_FOLDER)
    styles, data = {}, {}
    styles["openwindenergy"] = {
      "style": "openwindenergy.json",
      "tilejson": {
        "type": "overlay",
        "bounds": clipping_bounds
      }
    }
    styles["openmaptiles"] = {
      "style": "openmaptiles.json",
      "tilejson": {
        "type": "overlay",
        "bounds": clipping_bounds
      }
    }
    data["openmaptiles"] = {
      "mbtiles": basename(basemap_mbtiles)
    }

    custom_configuration_file_prefix = ''
    if CUSTOM_CONFIGURATION is not None: custom_configuration_file_prefix = CUSTOM_CONFIGURATION_FILE_PREFIX

    # Insert overall constraints as first item in list so it appears as first item in tileserver-gl
    overallconstraints = getFinalLayerLatestName(FINALLAYERS_CONSOLIDATED) + '.geojson'

    if overallconstraints in output_files: output_files.remove(overallconstraints)
    if not isfile(FINALLAYERS_OUTPUT_FOLDER + overallconstraints): LogFatalError("Final overall constraints layer is missing")

    # Set prefix for only those files we're interested in processing with Tippecanoe
    required_prefix = custom_configuration_file_prefix + LATEST_OUTPUT_FILE_PREFIX

    # Tippecanoe is used to create mbtiles for all 'latest--...' / 'custom--latest...' GeoJSONs

    queue_index, queue_dict = 0, {}
    output_files.insert(0, overallconstraints)
    for output_file in output_files:
        queue_index += 1

        # Only process GeoJSONs with required_prefix
        if (not output_file.startswith(required_prefix)) or (not output_file.endswith('.geojson')): continue

        # derived_dataset_name will begin with required_prefix, ie. 'latest--'
        # or 'custom--latest--' as we've specifically filtered on required_prefix
        derived_dataset_name = basename(output_file).replace('.geojson', '')

        # Don't process any datasets that are not in dataset_style_lookup (flat list of all used outputted datasets)
        if derived_dataset_name not in dataset_style_lookup: continue

        # original_table_name for all outputs will begin 'tip-...'
        # as we store pre-output geometries with these specific table names
        original_table_name = getOutputFileOriginalTable(output_file)

        # core_dataset_name refers to essential dataset, eg. 'scheduled-ancient-monuments'
        # or 'ecology-and-wildlife', which is shared between non-custom and custom modes
        # and also across some early-stage and pre-output database tables.
        # For example:
        # derived_dataset_name = 'custom--latest--ecology-and-wildlife'
        # core_dataset_name = 'ecology-and-wildlife'
        core_dataset_name = getCoreDatasetName(derived_dataset_name)

        tippecanoe_output = TILESERVER_DATA_FOLDER + output_file.replace('.geojson', '.mbtiles')

        style_id = derived_dataset_name
        style_name = dataset_style_lookup[derived_dataset_name]['title']

        # If tippecanoe failed previously for any reason, delete the output and intermediary file

        tippecanoe_interrupted_file = tippecanoe_output + '-journal'
        if isfile(tippecanoe_interrupted_file):
            os.remove(tippecanoe_interrupted_file)
            if isfile(tippecanoe_output): os.remove(tippecanoe_output)

        # Create grid-clipped version of GeoJSON to input into tippecanoe to improve mbtiles rendering and performance

        if not isfile(tippecanoe_output):

            tippecanoe_grid_clipped_file = join(TEMP_FOLDER,  'tippecanoe--grid-clipped--' + core_dataset_name + '.geojson')
            queue_parameters =  { \
                                    'dataset_name': core_dataset_name, \
                                    'derived_dataset_name': derived_dataset_name, \
                                    'table_name': original_table_name, \
                                    'style_name': style_name, \
                                    'tippecanoe_input': tippecanoe_grid_clipped_file, \
                                    'tippecanoe_output': tippecanoe_output
                                }
            
            priority = os.path.getsize(join(FINALLAYERS_OUTPUT_FOLDER, output_file))
            queue_dict_index = getQueueKey(priority, queue_index)
            queue_dict[queue_dict_index] = queue_parameters

        LogMessage("Created tileserver-gl style file for: " + output_file)

        style_color = dataset_style_lookup[derived_dataset_name]['color']
        style_level = dataset_style_lookup[derived_dataset_name]['level']
        style_defaultactive = dataset_style_lookup[derived_dataset_name]['defaultactive']
        style_opacity = 0.8 if style_level == 1 else 0.5
        style_file = TILESERVER_STYLES_FOLDER + style_id + '.json'
        style_json = {
            "version": 8,
            "id": style_id,
            "name": style_name,
            "sources": {
              	derived_dataset_name: {
                    "type": "vector",
                    "buffer": 512,
                    "url": TILESERVER_URL + "/data/" + style_id + ".json",
                    "attribution": attribution
                }
            },
            "glyphs": fonts_url,
            "layers": [
                {
                    "id": style_id,
                    "source": style_id,
                    "source-layer": style_id,
                    "type": "fill",
                    "paint": {
                        "fill-opacity": style_opacity,
                        "fill-color": style_color
                    }
                }
            ]
        }

        openwind_style_json['sources'][style_id] = style_json['sources'][derived_dataset_name]
        with open(style_file, "w") as json_file: json.dump(style_json, json_file, indent=4)

        openwind_layer = style_json['layers'][0]
        # Temporary workaround as setting 'fill-outline-color'='#FFFFFF00' on individual style breaks WMTS
        openwind_layer['paint']['fill-outline-color'] = "#FFFFFF00"
        if style_defaultactive: openwind_layer['layout'] = {'visibility': 'visible'}
        else: openwind_layer['layout'] = {'visibility': 'none'}

        # Hide overall constraint layer
        if core_dataset_name == FINALLAYERS_CONSOLIDATED: openwind_layer['layout'] = {'visibility': 'none'}

        openwind_style_json['layers'].append(openwind_layer)

        styles[style_id] = {
            "style": basename(style_file),
            "tilejson": {
                "type": "overlay",
                "bounds": clipping_bounds
            }
        }
        data[style_id] = {
            "mbtiles": basename(tippecanoe_output)
        }

    # Run multiprocessing to create Tippecanoe grid-sliced input files

    num_items_to_process = Value('i', len(queue_dict))
    queue_dict = dict(sorted(queue_dict.items(), reverse=True))
    queue_items = [queue_dict[item] for item in queue_dict]
    chunksize = 1

    multiprocessBefore()

    with Pool(processes=getNumberProcesses(), initializer=init_globals_count, initargs=(num_items_to_process, )) as p:
        p.map(createGridClippedFile, queue_items, chunksize=chunksize)

    multiprocessAfter()

    with open(openwind_style_file, "w") as json_file: json.dump(openwind_style_json, json_file, indent=4)

    # Creating final tileserver-gl config file

    config_file = TILESERVER_FOLDER + 'config.json'
    if use_font_folder:
        config_json = {
            "options": {
                "paths": {
                "root": "",
                "fonts": "fonts",
                "sprites": "sprites",
                "styles": "styles",
                "mbtiles": "data"
                }
            },
            "styles": styles,
            "data": data
        }
    else:
        config_json = {
            "options": {
                "paths": {
                "root": "",
                "sprites": "sprites",
                "styles": "styles",
                "mbtiles": "data"
                }
            },
            "styles": styles,
            "data": data
        }

    for item in queue_items:
            
        LogMessage("Creating mbtiles for: " + item['table_name'])

        inputs = runSubprocess(["tippecanoe", \
                                "-Z4", "-z15", \
                                "-X", \
                                "--generate-ids", \
                                "--force", \
                                "-n", item['style_name'], \
                                "-l", item['derived_dataset_name'], \
                                item['tippecanoe_input'], \
                                "-o", item['tippecanoe_output'] ])

        if isfile(item['tippecanoe_input']): os.remove(item['tippecanoe_input'])

        if not isfile(item['tippecanoe_output']):
            LogError("Failed to create mbtiles: " + basename(item['tippecanoe_output']))
            LogFatalError("*** Aborting process *** ")

    with open(config_file, "w") as json_file: json.dump(config_json, json_file, indent=4)

    if isdir(TEMP_FOLDER): shutil.rmtree(TEMP_FOLDER)

    LogMessage("All tileserver files created")



