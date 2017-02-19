import happybase
from socket import error as socket_error

CONNECTION_STRING_TEMPLATE = '{0}.service.production'
TIMEOUT_RETRIES = 5

def get_hbase_table(table_name, cluster_name='hbase-mrp'):
    return happybase.Connection(CONNECTION_STRING_TEMPLATE.format(cluster_name)).table(table_name)

def validate_records_per_region(table_name, columns = None, minimum_regions_count = 1, rows_per_region = 1000, cluster_name = 'hbase-mrp'):
    print 'checking validity of hbase table: %s' % table_name

    tbl = get_hbase_table(table_name, cluster_name)

    regions = tbl.regions()
    if (len(regions) < minimum_regions_count):
        print 'too few regions in table (%d < %d)' % (len(regions), minimum_regions_count)
        return False

    #skipping last region, it can have few keys
    for region in regions[:-1]:
        start_key = region['start_key']
        sc = tbl.scan(row_start=start_key, columns=columns, limit=rows_per_region)
        for i in range(rows_per_region):
            timeout_retry = 0

            try:
                next = sc.next()
            except StopIteration:
                print "to few keys in region: %s\n starting key: %s\n (%d < %d)" % (region['name'], start_key, i, rows_per_region)
                return False
            except socket_error:
                timeout_retry += 1
                if timeout_retry < TIMEOUT_RETRIES:
                    print 'socket timeout in region number %d. reloading table (retry %d)' % (i, timeout_retry)
                    tbl = get_hbase_table(table_name, cluster_name)
                    sc = tbl.scan(row_start=start_key, columns=columns, limit=rows_per_region - i)
                else:
                    print 'could not read table'
                    return False

    print 'table is ok'
    return True
