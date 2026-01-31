
# Efficient clipping logic

        LogMessage(prefix + source_table + ": Clipping partially overlapping polygons")

        postgisExec("""
        CREATE TABLE %s AS 
            SELECT ST_Intersection(clipping.geom, data.geom) geom
            FROM %s data, %s clipping 
            WHERE 
                (NOT ST_Contains(clipping.geom, data.geom) AND 
                ST_Intersects(clipping.geom, data.geom));""", \
            (AsIs(scratch_table_2), AsIs(scratch_table_1), AsIs(clipping_union_table), ))

        LogMessage(prefix + source_table + ": Adding fully enclosed polygons")

        postgisExec("""
        INSERT INTO %s  
            SELECT data.geom  
            FROM %s data, %s clipping 
            WHERE 
                ST_Contains(clipping.geom, data.geom);""", \
            (AsIs(scratch_table_2), AsIs(scratch_table_1), AsIs(clipping_union_table), ))

# As we're keeping grid-square data throughout all processing, we need to add an ST_Union during 'postprocess' before any datasets are exported

            LogMessage(prefix + source_table + ": Processing grid square " + str(grid_square_index + 1) + "/" + str(grid_square_count))

            postgisExec("""
            INSERT INTO %s 
                SELECT 
                    grid.id, 
                    (ST_Dump(ST_Union(ST_Intersection(grid.geom, dataset.geom)))).geom geom 
                FROM %s grid, %s dataset 
                WHERE grid.id = %s AND ST_geometrytype(dataset.geom) = 'ST_Polygon' GROUP BY grid.id""", (AsIs(processed_table), AsIs(processing_grid), AsIs(scratch_table_3), AsIs(grid_square_id), ))

        postgisExec("CREATE INDEX %s ON %s USING GIST (geom);", (AsIs(processed_table + "_idx"), AsIs(processed_table), ))

        if postgisCheckTableExists(scratch_table_1): postgisDropTable(scratch_table_1)
        if postgisCheckTableExists(scratch_table_2): postgisDropTable(scratch_table_2)
        if postgisCheckTableExists(scratch_table_3): postgisDropTable(scratch_table_3)

    with global_count.get_lock():
        global_count.value -= 1
        LogMessage(prefix + "FINISHED: Processed table: " + processed_table + " [" + str(global_count.value) + " dataset(s) to be processed]")