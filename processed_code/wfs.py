def guessWFSLayerIndex(layers):
    """
    Get WFS index from array of layers
    We check the title of the layer to see if if has 'boundary' or 'boundaries' in it - if so, select
    """

    layer_index = 0
    for layer in layers:
        if 'Title' in layer:
            if 'boundary' in layer['Title'].lower(): return layer_index
            if 'boundaries' in layer['Title'].lower(): return layer_index
        layer_index += 1

    return 0







        temp_output_file = temp_base + '.gpkg'
        getfeature_url = dataset['url']
        # We need DOWNLOAD_USER_AGENT 'User-Agent' header to allow access to scot.gov's WFS AWS servers
        # Following direct communication with data providers (12/05/2025), 
        # they added DOWNLOAD_USER_AGENT ('openwindenergy/*') as exception to their blacklist
        LogMessage("Setting 'User-Agent' to " + DOWNLOAD_USER_AGENT + " to enable WFS download from specific data providers")
        headers = {'User-Agent': DOWNLOAD_USER_AGENT}

        # Attempt to connect to WFS using highest version

        wfs_version = '2.0.0'
        try:
            wfs = WebFeatureService(url=dataset['url'], version='2.0.0', headers=headers)
        except:
            try:
                wfs = WebFeatureService(url=dataset['url'], headers=headers)
                wfs_version = wfs.version
            except:
                LogError("Problem accessing WFS: " + getfeature_url)
                with global_boolean.get_lock(): global_boolean.value = 0
                return

        # Get correct url for 'GetFeature' as this may different from
        # initial url providing capabilities information

        methods = wfs.getOperationByName('GetFeature').methods
        for method in methods:
            if method['type'].lower() == 'get': getfeature_url = method['url']

        # We default to first available layer in WFS
        # If different layer is needed, set 'layer' custom field in CKAN

        layers = list(wfs.contents)
        layer = layers[0]
        if ('layer' in dataset) and (dataset['layer'] is not None): layer = dataset['layer']

        # Extract CRS from WFS layer info

        crs = str(wfs[layer].crsOptions[0]).replace('urn:ogc:def:crs:', '').replace('::', ':').replace('OGC:1.3:CRS84', 'EPSG:4326')

        # Perform initial 'hits' query to get total records and pagination batch size

        params={
            'SERVICE': 'WFS',
            'VERSION': wfs_version,
            'REQUEST': 'GetFeature',
            'RESULTTYPE': 'hits',
            'TYPENAME': layer
        }
        url = getfeature_url.split('?')[0] + '?' + urllib.parse.urlencode(params)
        response = requests.get(url, headers=headers)
        result = xmltodict.parse(response.text)

        # Return False if incorrect response so we can retry again

        if not ('wfs:FeatureCollection' in result):
            LogError("Missing wfs:FeatureCollection in response from: " + getfeature_url)
            with global_boolean.get_lock(): global_boolean.value = 0
            return

        if not ('@numberMatched' in result['wfs:FeatureCollection']):
            LogError("Missing @numberMatched in response from: " + getfeature_url)
            with global_boolean.get_lock(): global_boolean.value = 0
            return

        if not ('@numberReturned' in result['wfs:FeatureCollection']):
            LogError("Missing @numberReturned in response from: " + getfeature_url)
            with global_boolean.get_lock(): global_boolean.value = 0
            return

        totalrecords = int(result['wfs:FeatureCollection']['@numberMatched'])
        batchsize = int(result['wfs:FeatureCollection']['@numberReturned'])

        # If batchsize is 0, suggests that there is no limit so attempt to load all records

        if batchsize == 0: batchsize = totalrecords

        # Download data page by page

        LogMessage("Downloading WFS:     " + feature_name+ " [records: " + str(totalrecords) + "]")

        dataframe, startIndex, recordsdownloaded = None, 0, 0

        while True:

            recordstodownload = totalrecords - recordsdownloaded
            if recordstodownload > batchsize: recordstodownload = batchsize

            wfs_request_url = Request('GET', getfeature_url, headers=headers, params={
                'service': 'WFS',
                'version': wfs_version,
                'request': 'GetFeature',
                'typename': layer,
                'count': recordstodownload,
                'startIndex': startIndex,
            }).prepare().url

            LogMessage("--> Downloading: " + str(startIndex + 1) + " to " + str(startIndex + recordstodownload))

            try:
                dataframe_new = gpd.read_file(wfs_request_url).set_crs(crs)

                if dataframe is None: dataframe = dataframe_new
                else: dataframe = pd.concat([dataframe, dataframe_new])

                recordsdownloaded += recordstodownload
                startIndex += recordstodownload

                if recordsdownloaded >= totalrecords: break
            except:
                LogMessage("--> Unable to download records - possible incorrect record count from WFS [numberMatched:" + str(totalrecords) + ", numberReturned:" + str(batchsize) + "] - retrying with reduced number")

                recordstodownload -= 1
                totalrecords -= 1
                if recordstodownload == 0: break

        dataframe.to_file(temp_output_file)

        with global_count.get_lock(): global_count.value += 1