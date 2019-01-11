__author__ = 'Felix'

import argparse
import json
import urllib
import logging
import re
from pylib.config.SnowflakeConfig import SnowflakeConfig


env_history_server = SnowflakeConfig().get_service_name(service_name='datacol-history-server')
job_history_server = 'http://' + env_history_server
job_history_port = 19888
in_xlhost = False
if "hdfs-namenode" in env_history_server: 
     in_xlhost = True

job_endpoint = '%(server)s:%(port)d/ws/v1/history/mapreduce/jobs/%(job_id)s'
tasks_endpoint = '%(server)s:%(port)d/ws/v1/history/mapreduce/jobs/%(job_id)s/tasks'
attempts_endpoint = '%(server)s:%(port)d/ws/v1/history/mapreduce/jobs/%(job_id)s/tasks/%(task_id)s/attempts'

attempt_log_url = '%(server)s:%(port)d/jobhistory/logs/%(node)s:%(log_port)d/%(container)s/%(attempt)s/%(user)s/syslog/?start=0'
log_port = 8041


def get_relative_file(split):
    if 'hdfs' in split:
        split = '/' + '/'.join(split[split.find('hdfs') + len('hdfs://'):].split('/')[1:])

    return split.split(':')[0]


LOG_PORT = 4545
log_url_re = re.compile(r"(.*/jobhistory/logs.*:)(\d+)(.*)")
def fix_log_url(unchecked_log_url):
    if in_xlhost:
        log_match = re.split(log_url_re, unchecked_log_url)
        log_match[2] = str(LOG_PORT)   # replace with correct port
        return ''.join(log_match)
    else:
        return unchecked_log_url


corrupt_input_indicators = [
    'Unexpected end of input stream',
    'incorrect data check',
    'invalid code lengths set',
    'too many length or distance symbols'
]


def get_bad_splits(log_str):
    splits = None
    bad_splits = []
    for line in log_str:
        if 'Processing split' in line:
            splits_str = line.split(' ')[-1]
            combined_input_qualifier = 'Paths:'
            splits = [get_relative_file(split) for split in splits_str[len(combined_input_qualifier):].split(',')] if \
                splits_str.startswith(combined_input_qualifier) else [get_relative_file(splits_str)]
        if any([msg in line for msg in corrupt_input_indicators]) and splits is not None:
            bad_splits += splits

    return bad_splits


def get_corrupt_input_files(job_id):
    logging.info('checking job %s' % job_id)

    job_url = job_endpoint % {'server': job_history_server, 'port': job_history_port, 'job_id': job_id}
    resp = json.load(urllib.urlopen(job_url))
    user = resp['job']['user']
    logging.info('user is %s' % user)

    tasks_url = tasks_endpoint % {'server': job_history_server, 'port': job_history_port, 'job_id': job_id}
    resp = json.load(urllib.urlopen(tasks_url))
    tasks = resp['tasks']['task']
    failed_maps = [task for task in tasks if task['type'] == 'MAP' and task['state'] == 'FAILED']
    if len(failed_maps) > 0:
        logging.info('there have been %d failed map attempts, looking to see if the reason was bad input stream' % len(
            failed_maps))

    corrupt_files = set()

    for task in failed_maps:
        attempts_url = attempts_endpoint % {'server': job_history_server, 'port': job_history_port,
                                            'job_id': job_id, 'task_id': task['id']}
        resp = json.load(urllib.urlopen(attempts_url))
        if resp is None:
            continue
        attempts = (resp.get('taskAttempts', None) or {}).get('taskAttempt', [])
        for attempt in attempts:
            if attempt['state'] == 'FAILED' and any([msg in attempt['diagnostics'] for msg in corrupt_input_indicators]):
                logging.info('attempt failed due to input stream issues')
                log_url = attempt_log_url % {'server': job_history_server, 'port': job_history_port,
                                             'node': attempt['nodeHttpAddress'].split(':')[0],
                                             'container': attempt['assignedContainerId'], 'log_port': log_port,
                                             'attempt': attempt['id'], 'user': user}

                log = urllib.urlopen(fix_log_url(log_url))
                for bad_split in get_bad_splits(log):
                    corrupt_files.add(bad_split)

    return corrupt_files


if __name__ == '__main__':
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('job_id', metavar='JOB_ID', help='Job ID')

    args = parser.parse_args()

    corrupt_files = get_corrupt_input_files(args.job_id)
    if len(corrupt_files) > 0:
        logging.info('corrupt files detected:')
        print ' '.join(corrupt_files)


