__author__ = 'Felix'

import json
import urllib


apps_by_tag_template = '%(server)s:%(port)d/ws/v1/cluster/apps?applicationTags=%(job_tag)s'


def find_applications_by_tag(rm_host, rm_port, tag):
    job_url = apps_by_tag_template % {'server': rm_host, 'port': rm_port, 'job_tag': tag}
    resp = json.load(urllib.urlopen(job_url))
    return [app['id'] for app in resp['apps']['app']]
