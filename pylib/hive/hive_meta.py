import psycopg2
import urlparse
import re
import logging

logger = logging.getLogger('ptask')
logger.addHandler(logging.StreamHandler())

HIVE_METASTORE_DB_DEFAULT = 'hive'
HIVE_METASTORE_PORT_DEFAULT = 5432

HIVE_METASTORE_CONN_STR_CONSUL_KEY = 'services/hive-metastore-connection-string'
HIVE_METASTORE_DB_CONSUL_KEY = 'services/hive-metastore-schema'
HIVE_METASTORE_PORT_CONSUL_KEY = 'services/hive-metastore-port'

HDFS_PATH_RE = re.compile('hdfs://([^/])*(/.*)')


def hdfs_branch_re(relative_location):
    return re.compile('hdfs://([^/])*%s($|/.*)' % relative_location)


def extract_relative_path(qualified_hdfs_uri):
    return HDFS_PATH_RE.search(qualified_hdfs_uri).group(2)


def _sql_like(path):
    return '%' + path + '%'


def _db_conn():
    from pylib.sw_config.bigdata_kv import get_kv
    kv = get_kv()

    connection_string = kv.get(HIVE_METASTORE_CONN_STR_CONSUL_KEY)
    database = kv.get(HIVE_METASTORE_DB_CONSUL_KEY) or HIVE_METASTORE_DB_DEFAULT
    port = kv.get(HIVE_METASTORE_PORT_CONSUL_KEY) or HIVE_METASTORE_PORT_DEFAULT

    if not isinstance(port, (int, long)):
        port = int(port)

    logging.info('Hive metastore connection string: ' + connection_string)
    logging.info('Hive metastore port: ' + str(port))

    conn_conf = urlparse.urlparse(connection_string)
    # if postgreSQL 9.2 is install, can initiate connection directly with connection string. Check back in the future

    if connection_string.startswith('postgresql'):
        return psycopg2.connect(database=database, user=conn_conf.username,
                                password=conn_conf.password, host=conn_conf.hostname, port=port)
    else:
        import MySQLdb
        return MySQLdb.connect(host=conn_conf.hostname, port=port, user=conn_conf.username, passwd=conn_conf.password, db=database)


def _fetch_all(conn, query, term):
    cursor = None

    # Cannot be done with 'with' statement: MySQL extracts cursor and Postgres does not, so there
    # should be 1 'with' for mysql and 2 'with' for postgres
    try:
        cursor = conn.cursor()
        fixed_query = check_quotting(conn, query)
        cursor.execute(fixed_query, term)
        return cursor.fetchall()
    finally:
        if cursor is not None:
            cursor.close()
        conn.close()


def check_quotting(conn, query):
    if 'escape_string' in dir(conn):
        import string
        return string.replace(query, '"', '`')
    else:
        return query


def get_table_location(hive_table):
    db_name, table_name = hive_table.split('.')
    location_query = 'SELECT s."LOCATION" location_uri ' \
                     'FROM "TBLS" t ' \
                     'JOIN "SDS" s using ("SD_ID") ' \
                     'JOIN "DBS" d using ("DB_ID") ' \
                     'WHERE d."NAME"=%s AND t."TBL_NAME"=%s'
    res = _fetch_all(_db_conn(), location_query, [db_name, table_name])
    return extract_relative_path(res[0][0])


def get_tables_by_location(location, verbose=False):
    find_query = 'SELECT s."LOCATION" location_uri, d."NAME" db_name, t."TBL_NAME" table_name ' \
                 'FROM "TBLS" t ' \
                 'JOIN "SDS" s using ("SD_ID") ' \
                 'JOIN "DBS" d using ("DB_ID") ' \
                 'WHERE s."LOCATION" LIKE %s'
    search_term = _sql_like(location)
    res = _fetch_all(_db_conn(), find_query, [search_term])
    potential_matches = res

    match_re = hdfs_branch_re(location)
    return [(db, table) for (table_loc, db, table) in potential_matches if match_re.match(table_loc) is not None]
