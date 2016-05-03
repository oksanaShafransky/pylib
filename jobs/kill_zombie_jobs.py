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

job_rm = 'http://active.yarn-rm-mrp.service.production'
job_rm_port = 8088
valid_states = 'SUBMITTED,ACCEPTED,RUNNING'
user = 'airflow'

zombie_apps_template = '%(server)s:%(port)d/ws/v1/cluster/apps?user=%(user)s&states=%(states)s&limit=1&applicationTags=%(task_id)s'
kill_app_template = '%(server)s:%(port)d/ws/v1/cluster/apps/%(app_id)s/state'


def kill_zombie_jobs(task_id):
    logger.info('checking if jobs with Airflow unique identifier %s is running...' % args.task_id)

    job_url = zombie_apps_template % {'server': job_rm, 'port': job_rm_port, 'user': user, 'states': valid_states,
                                      'task_id': args.task_id}
    resp = json.load(urllib.urlopen(job_url))
    apps = resp['apps']

    if apps is not None:
        try:
            id = apps['app'][0]['id']
            logger.info('found and killing %s...' % id)

            app_kill_url = kill_app_template % {'server': job_rm, 'port': job_rm_port, 'app_id': id}
            r = requests.put(app_kill_url, data=json.dumps({'state': 'KILLED'}))
            r.raise_for_status()

        except Exception as e:
            logger.error('could not kill zombie job')
            logger.error(traceback.format_exc())
            sys.exit(1)
    else:
        logger.info('no zombie job found')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('task_id', metavar='TASK_ID', help='Airflow Unique Task ID')

    args = parser.parse_args()
    kill_zombie_jobs(args.task_id)
