from airflow.models import Variable, Connection
from airflow.utils import *


def set_default_vars():
    Variable.set("hbase_deploy_targets", ["hbp1", "hbp2"], serialize_json=True)


def set_default_connections():
    merge_conn(
            Connection(
                    conn_id='etcd_default ',
                    conn_type='etcd',
                    host='10.0.5.11',
                    port='4001'))

if __name__ == "__main__":
    set_default_vars()
    set_default_connections()