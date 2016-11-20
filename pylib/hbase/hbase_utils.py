import happybase
CONNECTION_STRING_TEMPLATE = '{0}.service.production'

def validate_records_per_region(table_name, minimum_regions_count = 100, rows_per_region = 50, cluster_name = 'hbase-mrp'):
    conn = happybase.Connection(CONNECTION_STRING_TEMPLATE.format(cluster_name))
    tbl = conn.table(table_name)
    regions = tbl.regions()
    if (len(regions) < minimum_regions_count):
        return False

    for region in regions:
        start_key = region['start_key']
        sc = tbl.scan(row_start=start_key)
        for i in range(rows_per_region):
            try:
                next = sc.next()
            except:
                return False
    return True