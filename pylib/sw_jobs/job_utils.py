__author__ = 'Felix'

import json
import os
import sys
import hashlib
import logging
import base64

import requests

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(__name__)


def fetch_yarn_applications(rm_host, rm_port, tags=None, user=None, states=None):
    job_url = 'http://%(host)s:%(port)s/ws/v1/cluster/apps?' % {'host': rm_host, 'port': rm_port}
    queries = []
    if tags:
        assert len(tags) > 0, "You must provide tags, got %s" % tags
        # applicationTags - applications matching ANY! of the given application tags - so we can use only 1 tag
        tag_key, tag_value = tags.items()[0]
        queries.append('applicationTags?%s=%s' % (tag_key, tag_value))
    if user:
        queries.append("user=%s" % user)
    if states:
        queries.append("states=" + ','.join(states))
    if len(queries) > 0:
        job_url = job_url + "?" + "&".join(queries)
    r = requests.get(job_url, headers={"content-type": "application/json"})
    r.raise_for_status()
    apps = r.json()['apps']['app'] if r.json()['apps'] else []
    tags_set = set(tags.items()) if tags else set()
    return [app for app in apps if tags_set.issubset(set(parse_yarn_tags_str_to_dict(app['applicationTags']).items()))]


def kill_yarn_application(rm_host, rm_port, application_id):
    app_kill_url = 'http://%(host)s:%(port)d/ws/v1/cluster/apps/%(app_id)s/state' \
                   % {'host': rm_host, 'port': rm_port, 'app_id': application_id}
    r = requests.put(app_kill_url, headers={"content-type": "application/json"},
                     data=json.dumps({'state': 'KILLED'}))
    r.raise_for_status()


def yarn_tags_dict_to_str(yarn_tags):
    yarn_application_tags_str = sorted(["%s:%s" % (key, value) for key, value in yarn_tags.items()])
    # limit 100 characters (yarn-limit - maximum allowed length of a tag is 100)
    yarn_application_tags_str = ','.join([tag[:100] for tag in yarn_application_tags_str])
    return yarn_application_tags_str


def extract_yarn_application_tags_from_env():
    user = os.environ['USER_NAME'] if 'USER_NAME' in os.environ else ''
    task_id = os.environ['TASK_ID'] if 'TASK_ID' in os.environ else None

    if not task_id:
        logger.warning("yarn application should have a task-id ('TASK_ID' env var)")
        return dict()
    # generates a kill_tag that matches only jobs submitted by the same user - we may decide to change it
    # Use base64 and not HexDigits because 128bits in Hex its 32 digits and in base64 its only 24 digits
    kill_tag = base64.b64encode(hashlib.md5(user + task_id).digest())
    yarn_tags = {
        "kill_tag": kill_tag,
        "task_id": task_id,
    }

    snowflake_env = os.environ.get("SNOWFLAKE_ENV", None)
    if snowflake_env:
        yarn_tags["snowflake_env"] = snowflake_env

    if user == 'airflow':
        # parse airflow's tags from TASK_ID
        # TASK_ID = airflow.{DAG-ID}.{TASK-ID}.{EXECUTION-DATE}
        airflow_task_info = task_id.split(".")
        if len(airflow_task_info) == 4:
            yarn_tags.update({
                "airflow_dag_id": airflow_task_info[1],
                "airflow_task_id": airflow_task_info[2],
                "airflow_execution_date": airflow_task_info[3],
            })
        else:
            logger.warning("Cant parse airflow's tags from TASK_ID {task_id}".format(task_id=task_id))
    # yarn will modify the characters to the lower-form - do it here to be explicit
    return {key.lower(): value.lower() for key, value in yarn_tags.items()}


def parse_yarn_tags_str_to_dict(yarn_application_tags_str):
    # input string of comma-separated tags: "tag1:value1,tag2:value2,..."
    # output - a lookup dict fot the tags { 'tag1': 'value1' , 'tag2' : 'value2' }
    yarn_application_tags = [tag_str.split(":") for tag_str in yarn_application_tags_str.split(",")]
    return {tag[0]: tag[1] if len(tag) == 2 else None for tag in yarn_application_tags}
