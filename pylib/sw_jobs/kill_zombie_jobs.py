__author__ = 'Amit'

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
user = 'airflow'  # kill only airflow's jobs

zombie_apps_template = 'http://%(server)s:%(port)d/ws/v1/cluster/apps?user=%(user)s&states=%(states)s&applicationTags=%(app_tags)s'
kill_app_template = 'http://%(server)s:%(port)d/ws/v1/cluster/apps/%(app_id)s/state'


class ZombieJobKiller(object):

    def __init__(self, rm_server, rm_port=8088):
        self.server, self.port = rm_server, rm_port

    class ZombieHandleMode(object):
        alert = 0
        kill = 1

    def kill_zombie_jobs(self, app_tags, handling_mode=ZombieHandleMode.kill):
        logger.info('checking if jobs that matches tags: %s are running...' % app_tags)

        job_url = zombie_apps_template % {'server': self.server, 'port': self.port,
                                          'user': user,
                                          'states': valid_states,
                                          'app_tags': app_tags}
        resp = json.load(urllib.urlopen(job_url))
        apps = resp['apps']

        if apps is not None:
            try:
                for app in apps['app']:
                    zombie_app_id = app['id']
                    logger.info('zombie job found, killing application id:%s...' % zombie_app_id)
                    # For transition period, fail the job
                    if handling_mode == ZombieJobKiller.ZombieHandleMode.alert:
                        raise ZombieJobFoundException(app_tags)
                    elif handling_mode == ZombieJobKiller.ZombieHandleMode.kill:
                        app_kill_url = kill_app_template % {'server': self.server, 'port': self.port,
                                                            'app_id': zombie_app_id}
                        r = requests.put(app_kill_url, headers={"content-type": "application/json"},
                                         data=json.dumps({'state': 'KILLED'}))
                        r.raise_for_status()
                    else:
                        raise ValueError("Unknown zombie handle mode: %s" % handling_mode)

            except Exception:
                logger.error('could not kill zombie job')
                logger.error(traceback.format_exc())
                raise
        else:
            logger.info('no zombie job found')


class ZombieJobFoundException(Exception):
    def __init___(self, task_tags):
        super(ZombieJobFoundException, self).__init__(self, "Zombie job found for app tags: %s" % task_tags)
