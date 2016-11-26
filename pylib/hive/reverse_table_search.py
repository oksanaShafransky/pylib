from __future__ import print_function
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
    print('\n'.join(map(lambda db_table: '%s.%s' % db_table, find_table_name(location, print_query))))


if __name__ == '__main__':
    if len(sys.argv) == 1:
        print('Huh? What path did you want to check?')
        exit(1)
    location = sys.argv[1]
    print_query = None
    if len(sys.argv) > 2:
        print_query = sys.argv[2]
    get_table_names(location=location, print_query=print_query)

# get_table_names(location='/similargroup/data/analytics/window/post-estimate/aggkey=sending-pages', print_query=True)
# get_table_names(location='/similargroup/data/mobile-analytics/daily/aggregate/aggkey=SiteCountrySourceKey', print_query=False)
