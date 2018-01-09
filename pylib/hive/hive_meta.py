import psycopg2
import urlparse
import re
import logging

logger = logging.getLogger('ptask')
logger.addHandler(logging.StreamHandler())

HIVE_METASTORE_CONN_STR_CONSUL_KEY = 'services/hive-metastore-connection-string'
HIVE_METASTORE_CONN_STR_DEFAULT = 'postgresql://readonly:readonly@hive-postgres-mrp.service.production/hive'
HIVE_METASTORE_PORT_CONSUL_KEY = 'services/hive-metastore-port'
HIVE_METASTORE_PORT_DEFAULT = 5432

HDFS_PATH_RE = re.compile('hdfs://([^/])*(/.*)')


def hdfs_branch_re(relative_location):
    return re.compile('hdfs://([^/])*%s($|/.*)' % relative_location)


def extract_relative_path(qualified_hdfs_uri):
    return HDFS_PATH_RE.search(qualified_hdfs_uri).group(2)


def _sql_like(path):
    return '%' + path + '%'


def _db_conn():
    from pylib.tasks.ptask_infra import TasksInfra
    kv = TasksInfra.kv()

    connection_string = kv.get(HIVE_METASTORE_CONN_STR_CONSUL_KEY) or HIVE_METASTORE_CONN_STR_DEFAULT
    port = kv.get(HIVE_METASTORE_PORT_CONSUL_KEY) or HIVE_METASTORE_PORT_DEFAULT

    logging.info('Hive metastore connection string: ' + connection_string)
    logging.info('Hive metastore port: ' + port)

    conn_conf = urlparse.urlparse(connection_string)
    # if postgreSQL 9.2 is install, can initiate connection directly with connection string. Check back in the future
    return psycopg2.connect(database=conn_conf.path[1:], user=conn_conf.username,
                            password=conn_conf.password, host=conn_conf.hostname, port=port)


def get_table_location(hive_table):
    db_name, table_name = hive_table.split('.')
    location_query = 'SELECT s."LOCATION" location_uri ' \
                     'FROM "TBLS" t ' \
                     'JOIN "SDS" s using ("SD_ID") ' \
                     'JOIN "DBS" d using ("DB_ID") ' \
                     'WHERE d."NAME"=%s AND t."TBL_NAME"=%s'
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(location_query, [db_name, table_name])
            return extract_relative_path(cur.fetchall()[0][0])


def get_tables_by_location(location, verbose=False):
    find_query = 'SELECT s."LOCATION" location_uri, d."NAME" db_name, t."TBL_NAME" table_name ' \
                 'FROM "TBLS" t ' \
                 'JOIN "SDS" s using ("SD_ID") ' \
                 'JOIN "DBS" d using ("DB_ID") ' \
                 'WHERE s."LOCATION" LIKE %s'
    search_term = _sql_like(location)
    with _db_conn() as conn:
        with conn.cursor() as cur:
            if verbose:
                import sys
                sys.stdout.write(cur.mogrify(find_query, [location]) + '\n')
            cur.execute(find_query, [search_term])
            potential_matches = cur.fetchall()

    match_re = hdfs_branch_re(location)
    return [(db, table) for (table_loc, db, table) in potential_matches if match_re.match(table_loc) is not None]
