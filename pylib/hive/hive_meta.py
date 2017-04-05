import psycopg2
import re

HIVE_METASTORE_CONN_STR = 'postgresql://readonly:readonly@hive-postgres-mrp.service.production:5432/hive'
HDFS_PATH_RE = re.compile('hdfs://([^/])*(/.*)')


def hdfs_branch_re(relative_location):
    return re.compile('hdfs://([^/])*%s($|/.*)' % relative_location)


def extract_relative_path(qualified_hdfs_uri):
    return HDFS_PATH_RE.search(qualified_hdfs_uri).group(2)


def _sql_like(path):
    return '%' + path + '%'


def get_table_location(hive_table):
    db_name, table_name = hive_table.split('.')
    location_query = 'SELECT location_uri FROM hive_table_location WHERE db_name=%s AND table_name=%s'
    with psycopg2.connect(HIVE_METASTORE_CONN_STR) as conn:
        with conn.cursor() as cur:
            cur.execute(location_query, [db_name, table_name])
            return extract_relative_path(cur.fetchall()[0][0])


def get_tables_by_location(location, verbose=False):
    find_query = 'SELECT location_uri, db_name, table_name FROM hive_table_location WHERE location_uri LIKE %s'
    search_term = _sql_like(location)
    with psycopg2.connect(HIVE_METASTORE_CONN_STR) as conn:
        with conn.cursor() as cur:
            if verbose:
                import sys
                sys.stdout.write(cur.mogrify(find_query, [location]) + '\n')
            cur.execute(find_query, [search_term])
            potential_matches = cur.fetchall()

    match_re = hdfs_branch_re(location)
    return [(db, table) for (table_loc, db, table) in potential_matches if match_re.match(table_loc) is not None]
