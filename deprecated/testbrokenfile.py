import json
import pyproj
from pyproj import Transformer, CRS
import sys

# Place imports at top as per instructions

def audit_geojson_projection(file_path, s_epsg, t_epsg):
    """
    Tests every feature for projection validity and reports exactly which ones fail.
    """
    # 1. Setup the math engine
    # 'always_xy' ensures we don't get tripped up by Lat/Lon vs Lon/Lat order
    transformer = Transformer.from_crs(f"EPSG:{s_epsg}", f"EPSG:{t_epsg}", always_xy=True)
    bounds = CRS.from_user_input(s_epsg).area_of_use.bounds
    
    print(f"--- Auditing {file_path} ---")
    print(f"Source Bounds (WGS84): {bounds}\n")

    with open(file_path, 'r') as f:
        data = json.load(f)

    errors = 0
    for i, feature in enumerate(data.get('features', [])):
        geom = feature.get('geometry')
        if not geom:
            continue

        coords = geom.get('coordinates')
        
        # This handles Points. For Polygons, you'd iterate through the rings.
        try:
            if geom['type'] == 'Point':
                test_coords = [coords]
            elif geom['type'] in ['LineString', 'MultiPoint']:
                test_coords = coords
            elif geom['type'] in ['Polygon', 'MultiLineString']:
                test_coords = coords[0] # Test the outer ring
            else:
                # Simplification for audit: test the first coordinate of the first part
                test_coords = [coords[0][0][0]] if geom['type'] == 'MultiPolygon' else []

            for pt in test_coords:
                # The actual "Math Test"
                x_out, y_out = transformer.transform(pt[0], pt[1])
                
                # Check for Infinite or NaN results which cause the GDAL error
                if any(map(lambda v: abs(v) == float('inf') or v != v, [x_out, y_out])):
                    raise ValueError("Transformation resulted in out-of-bounds coordinates.")

        except Exception as e:
            errors += 1
            print(f"❌ ERROR: Feature Index {i}")
            print(f"   Properties: {feature.get('properties')}")
            print(f"   Coords: {coords}")
            print(f"   Reason: {e}\n")

    if errors == 0:
        print("✅ All features passed projection test.")
    else:
        print(f"Audit complete. Found {errors} broken features.")
        sys.exit(1) # Stop the pipeline if errors are found

# Run the audit
audit_geojson_projection("build/downloads/listed-buildings--northern-ireland.geojson", 29902, 25830)