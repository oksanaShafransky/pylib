import psycopg2
import sys

__author__ = 'barakg'

"""
Purpose of this tool is to search from tables by their location
Example query:
    select db_name, table_name ,location_uri from hive_table_location limit 10
"""


def find_table_name(location, print_query=True):
    with psycopg2.connect("postgresql://readonly:readonly@hive-postgres-mrp.service.production:5432/hive") as conn:
        with conn.cursor() as cur:
            location = '%' + location + '%'
            qry = """select db_name, table_name from hive_table_location where location_uri like %s;"""
            if print_query:
                sys.stdout.write(cur.mogrify(qry, [location]) + '\n')
            cur.execute(qry, [location])
            return cur.fetchall()


def get_table_names(location, print_query):
    print '\n'.join(map(lambda db_table: '%s.%s' % db_table, find_table_name(location, print_query)))


get_table_names(location='/similargroup/data/analytics/window/post-estimate/aggkey=sending-pages', print_query=False)
