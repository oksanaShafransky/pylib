import requests
import os


class SnowflakeConfig:
    def_env = None

    def __init__(self, url='http://bigdata-snowflake-ds-aws-production.op-us-east-1.bigdata-grid.int.similarweb.io',
                 path_in_url="/serviceResolution"):
        self.base_url = url
        self.path_in_url = path_in_url
        try:
            self.def_env = os.environ['SNOWFLAKE_ENV']
        except Exception:
            print("ERROR: failed to read snowflake environment variable")

    def get_service_name(self, env=None, service_name=None):
        if env is None:
            env = self.def_env
        # Must be first line in the function
        service_args = locals().items()
        # Must be first line in the function
        # Clean self from list
        service_args = [v for v in service_args if v[0] != 'self']
        payload = {}

        for var, value in service_args:
            if value is not None:
                payload[var] = value
        r = requests.get(self.base_url + self.path_in_url, params=payload)
        return str(r.text)


