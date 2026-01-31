def convertSHP2GeoJSON(path_shp, path_geojson, dataset_name):
    """
    Convert SHP to GeoJSON using pyshp in low-memory way
    """

    reader = shapefile.Reader(path_shp)
    fields = reader.fields[1:]
    field_names = [field[0] for field in fields]
    geojson = open(path_geojson, "w")
    geojson.write('{"type": "FeatureCollection", "name": "' + dataset_name + '", "features": [')
    numrecords = len(list(reader.iterRecords()))
    recordcount = 0
    for sr in reader.iterShapeRecords():
        atr = dict(zip(field_names, sr.record))
        geom = sr.shape.__geo_interface__
        geojson.write(json.dumps(dict(type="Feature", geometry=geom, properties=atr)))
        recordcount += 1
        if recordcount != numrecords: geojson.write(",\n")
    geojson.write(']}')
    geojson.close()
