import requests


class DynamicServicesConfig:
    def __init__(self, url='http://bigdata-snowflake-ds-aws-production.op-us-east-1.bigdata-grid.int.similarweb.io', path_in_url="/serviceResolution"):
        self.base_url = url
        self.path_in_url = path_in_url

    def get_service_name(self, env=None, service_name=None):
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
        return r.text


services_config = DynamicServicesConfig()
