import hashlib

__author__ = 'Amit'

import argparse
import json
import urllib
import requests
import traceback
import sys
import logging
import os

logging.basicConfig(level=logging.INFO,
                    stream=sys.stdout)
logger = logging.getLogger(os.path.basename(__file__))

valid_states = 'SUBMITTED,ACCEPTED,RUNNING'
user = 'airflow'

zombie_apps_template = 'http://%(server)s:%(port)d/ws/v1/cluster/apps?user=%(user)s&states=%(states)s&applicationTags=%(task_tags)s'
kill_app_template = 'http://%(server)s:%(port)d/ws/v1/cluster/apps/%(app_id)s/state'


class ZombieKiller(object):

    def __init__(self, rm_server, rm_port=8088):
        self.server, self.port = rm_server, rm_port

    class ZombieHandleMode(object):
        alert = 0
        kill = 1

    def kill_zombie_jobs(self, task_id, handling_mode=ZombieHandleMode.kill):

        if not task_id:
            raise ValueError("task_id cannot be empty")

        m = hashlib.md5()
        hashed_id = m.update(task_id).hexdigest()
        task_tags = ','.join([hashed_id, task_id])

        logger.info('checking if jobs with Airflow unique identifier %s is running...' % task_tags)

        job_url = zombie_apps_template % {'server': self.server, 'port': self.port, 'user': user, 'states': valid_states,
                                          'task_tags': task_tags}
        resp = json.load(urllib.urlopen(job_url))
        apps = resp['apps']

        if apps is not None:
            try:
                for app in apps['app']:
                    id = app['id']
                    logger.info('found and killing %s...' % id)

                    # For transition period, fail the job
                    if handling_mode == ZombieKiller.ZombieHandleMode.alert:
                        raise ZombieJobFoundException(task_id)
                    elif handling_mode == ZombieKiller.ZombieHandleMode.kill:
                        app_kill_url = kill_app_template % {'server': self.server, 'port': self.port, 'app_id': id}
                        r = requests.put(app_kill_url, headers={"content-type": "application/json"}, data=json.dumps({'state': 'KILLED'}))
                        r.raise_for_status()
                    else:
                        raise ValueError("Unknown zombie handle mode: %s" % handling_mode)

            except Exception as e:
                logger.error('could not kill zombie job')
                logger.error(traceback.format_exc())
                raise
        else:
            logger.info('no zombie job found')


class ZombieJobFoundException(Exception):
    def __init___(self, task_id):
        super(ZombieJobFoundException, self).__init__(self, "Zombie job found for task id %s" % task_id)
