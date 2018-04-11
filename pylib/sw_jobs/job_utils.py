import hashlib

__author__ = 'Felix'

import json
import urllib


apps_by_tag_template = '%(server)s:%(port)d/ws/v1/cluster/apps?applicationTags=%(job_tag)s'


def find_applications_by_tag(rm_host, rm_port, tag):
    job_url = apps_by_tag_template % {'server': rm_host, 'port': rm_port, 'job_tag': tag}
    resp = json.load(urllib.urlopen(job_url))
    return [app['id'] for app in resp['apps']['app']]


def hash_task_full_id(task_full_id):
    m = hashlib.md5()
    m.update(task_full_id)
    hashed_task_full_id = m.hexdigest()
    return hashed_task_full_id


def extract_yarn_application_tags(task_full_id):

    execution_user = task_full_id.split('.')[0]
    dag_id = task_full_id.split('.')[1]
    # The following is parsing trickery to allow task ids to contain dots
    execution_dt = task_full_id.split('.')[-1]
    task_id = task_full_id.split('.', 2)[2].replace('.' + execution_dt, '')

    # We're using an hashed application tag when the full task_id
    # exceeds 100 characters because yarn limits the tags length.
    if len(task_full_id) <= 100:
        yarn_application_tags = task_full_id
    else:
        yarn_application_tags = \
            '{hashed_task_full_id},' \
            'execution_user:{execution_user},' \
            'dag_id:{dag_id},' \
            'task_id:{task_id},' \
            'execution_dt:{execution_dt}'.format(
                hashed_task_full_id=hash_task_full_id(task_full_id),
                execution_user=execution_user,
                dag_id=dag_id,
                task_id=task_id,
                execution_dt=execution_dt
            )
    return yarn_application_tags
