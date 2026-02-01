



    with open(openmaptiles_style_file_dst, "w") as json_file: json.dump(openmaptiles_style_json, json_file, indent=4)

    attribution = "Source data copyright of multiple organisations. For all data sources, see <a href=\"" + CKAN_URL + "\" target=\"_blank\">" + CKAN_URL.replace('https://', '') + "</a>"

    basemap_mbtiles = TILESERVER_DATA_FOLDER + basename(OSM_MAIN_DOWNLOAD).replace(".osm.pbf", ".mbtiles")

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


    output_files = getFilesInFolder(FINALLAYERS_OUTPUT_FOLDER)


    queue_index, queue_dict = 0, {}
    output_files.insert(0, overallconstraints)




