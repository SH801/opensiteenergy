



# *****************************************
# **** Will we need these below? ****
# *****************************************

# ***********************************************************
# ********** Application data structure functions ***********
# ***********************************************************

def generateOSMLookup(osm_data):
    """
    Generates OSM JSON lookup file
    """

    global OSM_LOOKUP

    with open(OSM_LOOKUP, "w") as json_file: json.dump(osm_data, json_file, indent=4)

def generateStructureLookups(ckanpackages):
    """
    Generates structure JSON lookup files including style files for map app
    """

    global CUSTOM_CONFIGURATION, BUILD_FOLDER, MAPAPP_FOLDER, STRUCTURE_LOOKUP, MAPAPP_JS_STRUCTURE, HEIGHT_TO_TIP, BLADE_RADIUS, FINALLAYERS_CONSOLIDATED, TILESERVER_URL

    makeFolder(BUILD_FOLDER)
    makeFolder(MAPAPP_FOLDER)

    structure_lookup = {}
    configuration = ''
    if CUSTOM_CONFIGURATION is not None: configuration = CUSTOM_CONFIGURATION['configuration']

    style_items = [
    {
        "title": "All constraint layers",
        "color": "darkgrey",
        "dataset": getFinalLayerLatestName(FINALLAYERS_CONSOLIDATED),
        "level": 1,
        "children": [],
        "defaultactive": False,
        'height-to-tip': formatValue(HEIGHT_TO_TIP),
        'blade-radius': formatValue(BLADE_RADIUS),
        'configuration': configuration
    }]

    for ckanpackage in ckanpackages.keys():
        ckanpackage_group = reformatDatasetName(ckanpackage)
        structure_lookup[ckanpackage_group] = []
        finallayer_name = getFinalLayerLatestName(ckanpackage_group)
        style_item =   {
                            'title': ckanpackages[ckanpackage]['title'],
                            'color': ckanpackages[ckanpackage]['color'],
                            'dataset': finallayer_name,
                            'level': 1,
                            'defaultactive': True,
                            'height-to-tip': formatValue(HEIGHT_TO_TIP),
                            'blade-radius': formatValue(BLADE_RADIUS)
                        }
        children = {}
        for dataset in ckanpackages[ckanpackage]['datasets']:
            dataset_code = reformatDatasetName(dataset['title'])
            dataset_parent = getDatasetParent(dataset_code)
            if dataset_parent not in children:
                children[dataset_parent] =   {
                                                'title': getDatasetParentTitle(dataset['title']),
                                                'color': ckanpackages[ckanpackage]['color'],
                                                'dataset': getFinalLayerLatestName(dataset_parent),
                                                'level': 2,
                                                'defaultactive': False,
                                                'height-to-tip': formatValue(HEIGHT_TO_TIP),
                                                'blade-radius': formatValue(BLADE_RADIUS)
                                            }
            structure_lookup[ckanpackage_group].append(dataset_code)
        style_item['children'] = [children[children_key] for children_key in children.keys()]
        # If only one child, set parent to only child and remove children
        if len(style_item['children']) == 1:
            style_item = style_item['children'][0]
            style_item['level'] = 1
            style_item['defaultactive'] = True
        style_items.append(style_item)
        structure_lookup[ckanpackage_group] = sorted(structure_lookup[ckanpackage_group])

    structure_hierarchy_lookup = {}
    for ckanpackage in structure_lookup.keys():
        structure_hierarchy_lookup[ckanpackage] = {}
        for dataset in structure_lookup[ckanpackage]:
            layer_parent = "--".join(dataset.split("--")[0:1])
            if layer_parent not in structure_hierarchy_lookup[ckanpackage]: structure_hierarchy_lookup[ckanpackage][layer_parent] = []
            structure_hierarchy_lookup[ckanpackage][layer_parent].append(dataset)

    javascript_content = """
var url_tileserver_style_json = '""" + TILESERVER_URL + """/styles/openwindenergy/style.json';
var openwind_structure = """ + json.dumps({\
        'tipheight': formatValue(HEIGHT_TO_TIP), \
        'bladeradius': formatValue(BLADE_RADIUS), \
        'configuration': configuration, \
        'datasets': style_items\
    }, indent=4) + """;"""

    with open(STRUCTURE_LOOKUP, "w") as json_file: json.dump(structure_hierarchy_lookup, json_file, indent=4)
    with open(STYLE_LOOKUP, "w") as json_file: json.dump(style_items, json_file, indent=4)
    with open(MAPAPP_JS_STRUCTURE, "w") as javascript_file: javascript_file.write(javascript_content)



