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




