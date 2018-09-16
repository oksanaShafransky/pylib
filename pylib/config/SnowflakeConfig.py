import requests
import os
import traceback
import json
import socket
import copy


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
        payload['stacktrace'] = traceback.format_exc()
        headers = {'content-type': 'application/json'}
        requests.post(self.base_url + self.client_error_path, data=json.dumps(payload),
                      headers=headers)

    def get_service_name(self, env=None, service_name=None):
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
            raise Exception("SnowflakeError: " + r.content + " code: " + str(r.status_code))

        return str(r.text)

