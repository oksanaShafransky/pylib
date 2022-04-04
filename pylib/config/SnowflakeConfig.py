import requests
import os
import traceback, sys
import json
import socket
import copy
import getpass

MRP = "mrp"
MRP_AWS = "mrp-aws"


# Printing full stack
def full_stack():
    try:
        exc = sys.exc_info()[0]
        stack = traceback.extract_stack()[:-1]  # last one would be full_stack()
        if not exc is None:  # i.e. if an exception is present
            del stack[-1]  # remove call of full_stack, the printed exception
            # will contain the caught exception caller instead
        trc = 'Traceback (most recent call last):\n'
        stackstr = trc + ''.join(traceback.format_list(stack))
        if not exc is None:
            stackstr += '  ' + traceback.format_exc().lstrip(trc)
        return stackstr
    except Exception:
        return "Failed to retrieve stacktrace"


class ConnectionWrapper:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self._conn.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.close()


class SnowflakeConfig:
    def_env = None
    base_payload = {'platform': 'Python',
                    'task_id': 'unknown'}

    def __init__(self, env=None,
                 url='http://bigdata-snowflake-ds-aws-production.op-us-east-1.bigdata-grid.int.similarweb.io',
                 path_in_url="/serviceResolution",
                 client_error_path="/clientError"):
        self.base_url = url
        self.path_in_url = path_in_url
        self.client_error_path = client_error_path
        self.base_payload['hostname'] = socket.gethostname()
        self.base_payload['user'] = getpass.getuser()
        if os.environ.get('TASK_ID') is not None:
            self.base_payload['task_id'] = os.environ.get('TASK_ID')
        try:
            if env is None:
                self.def_env = os.environ['SNOWFLAKE_ENV']
            else:
                self.def_env = env
        except Exception:
            err_msg = "ERROR: failed to read snowflake environment variable"
            self.__alert_server_on_error(err_msg)
            print(err_msg)
            raise

    def __alert_server_on_error(self, error_msg):
        payload = copy.deepcopy(self.base_payload)
        payload['msg'] = error_msg
        payload['stacktrace'] = full_stack()
        headers = {'content-type': 'application/json'}
        requests.post(self.base_url + self.client_error_path, data=json.dumps(payload),
                      headers=headers)

    def get_service_name(self, env=None, service_name=None, task_id=None):
        # TODO - should we cache the answer on the local-machine? improve performance
        if env is None:
            env = self.def_env
        # Must be first line in the function
        service_args = locals().items()
        # Must be first line in the function
        # Clean self from list
        service_args = [v for v in service_args if v[0] != 'self']
        payload = copy.deepcopy(self.base_payload)

        for var, value in service_args:
            if value is not None:
                payload[var] = value
        r = requests.get(self.base_url + self.path_in_url, params=payload)
        if r.status_code != 200:
            # We got error from snowflake server
            raise Exception("SnowflakeError: env="+env+" service_name="+service_name+" err:" + r.content + " code: " + str(r.status_code))

        return str(r.text)

    def get_sql_connection(self, env=None, service_name=None, task_id=None):
        sql_config = json.loads(self.get_service_name(env, service_name, task_id))
        #from mysql.connector import connection
        #return ConnectionWrapper(connection.MySQLConnection(host=sql_config['server'], user=sql_config['user'], passwd=sql_config['password'], database=sql_config.get('db', None)))
        import MySQLdb
        return MySQLdb.connect(host=sql_config['server'], user=sql_config['user'], passwd=sql_config['password'],
                               db=sql_config.get('db', None))
