import requests
import logging

from pylib.config.SnowflakeConfig import SnowflakeConfig


class BranchStackClient(object):
    def __init__(self, branch_stack_host):
        self.branch_stack_host = branch_stack_host

    @staticmethod
    def from_snowflake(env=None):
        sc = SnowflakeConfig(env)
        branch_stack_host = sc.get_service_name(service_name='branchstack')
        return BranchStackClient(branch_stack_host=branch_stack_host)

    def get_production_branch(self):
        request_url = '{host}/branches/production'.format(host=self.branch_stack_host)

        res = requests.get(request_url)
        try:
            res.raise_for_status()
            return res.json()["data"]["id"]
        except requests.exceptions.HTTPError as e:
            logging.error('failed to get production branch {}'.format(e))


