import psycopg2

__author__ = 'barakg'

"""
Purpose of this tool is to search from tables by their location
Example query:
    select db_name, table_name ,location_uri from hive_table_location limit 10
"""


def find_table_name(location):
    with psycopg2.connect("postgresql://readonly:readonly@hive-postgres-mrp.service.production:5432/hive") as conn:
        with conn.cursor() as cur:
            location = '%' + location + '%'
            qry = """select db_name, table_name from hive_table_location where location_uri like %s;"""
            print cur.mogrify(qry, [location])
            cur.execute(qry, [location])
            return cur.fetchall()


def get_table_names(location):
    print '\n'.join(map(lambda db_table: '%s.%s' % db_table, find_table_name(location)))


# column names:  db_name, table_name ,location_uri
get_table_names(location='daily/aggregate/aggkey=SiteCountrySourceKey')
