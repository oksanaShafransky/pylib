__author__ = 'Felix'

import json
import os
import sys
import urllib
import hashlib
import logging
import base64

logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger(os.path.basename(__file__))
apps_by_tag_template = '%(server)s:%(port)d/ws/v1/cluster/apps?applicationTags=%(job_tag)s'


def find_applications_by_tag(rm_host, rm_port, tag):
    job_url = apps_by_tag_template % {'server': rm_host, 'port': rm_port, 'job_tag': tag}
    resp = json.load(urllib.urlopen(job_url))
    return [app['id'] for app in resp['apps']['app']]


def extract_yarn_application_tags():
    user = os.environ['USER_NAME'] if 'USER_NAME' in os.environ else ''
    task_id = os.environ['TASK_ID'] if 'TASK_ID' in os.environ else None
    assert task_id, "yarn application must have a task-id ('TASK_ID' env var)"
    # generates a kill_tag that matches only jobs submitted by the same user - we may decide to change it
    # Use base64 and not HexDigits because 128bits in Hex its 32 digits and in base64 its only 24 digits
    kill_tag = base64.b64encode(hashlib.md5(user + task_id).digest())
    # uses shorter kill_tag - 12 chars should be enough
    # (the chances of 2 diff running-apps to have the same kill_tag are still ignorable)
    kill_tag = kill_tag[:12]

    yarn_application_tags = \
        "kill_tag:{kill_tag},"  \
        "task_id:{task_id}".format(
            kill_tag=kill_tag,
            task_id=task_id
        )
    # limit 100 characters (yarn-limit -maximum allowed length of a tag is 100)
    # yarn will modify the characters to the lower-form - do it here to be explicit
    yarn_application_tags = yarn_application_tags[:100].lower()
    logger.info("Tagging Yarn-Application: %s" % yarn_application_tags)
    return yarn_application_tags


def parse_yarn_tags_to_dict(yarn_application_tags):
    # input string of comma-separated tags: "tag1:value1,tag2:value2,..."
    # output - a lookup dict fot the tags { 'tag1': 'value1' , 'tag2' : 'value2' }
    return {t.split(":")[0]: t.split(":")[1] for t in yarn_application_tags.split(",")}
