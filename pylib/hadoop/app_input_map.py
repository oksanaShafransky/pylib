__author__ = 'Felix'

import argparse
import json
import urllib
import logging
import time
from datetime import datetime, timedelta
from monthdelta import monthdelta
import re
import pandas as pd

job_history_server = 'http://mrp-nn-a01.sg.internal'
job_history_port = 19888

jobs_endpoint = '%(server)s:%(port)d/ws/v1/history/mapreduce/jobs?%(qp)s'
job_endpoint = '%(server)s:%(port)d/ws/v1/history/mapreduce/jobs/%(job_id)s'
job_conf_endpoint = '%(server)s:%(port)d/ws/v1/history/mapreduce/jobs/%(job_id)s/conf'


def get_daily_jobs(run_date, user=None):
    logging.info('checking jobs')
    query_params = {
        'startedTimeBegin': int(time.mktime(run_date.timetuple())) * 1000,
        'startedTimeEnd': int(time.mktime((run_date + timedelta(days=1)).timetuple())) * 1000
    }

    if user is not None:
        query_params['user'] = user

    req = jobs_endpoint % {
        'server': job_history_server,
        'port': job_history_port,
        'qp': '&'.join(map(lambda kv: '%s=%s' % (kv[0], str(kv[1])), query_params.items()))
    }

    return json.load(urllib.urlopen(req))


full_date_re = re.compile('([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01]))')
short_date_re = re.compile('([12]\d{3}-(0[1-9]|1[0-2]))')


def determine_job_date(job_id):
    job_url = job_endpoint % {'server': job_history_server, 'port': job_history_port, 'job_id': job_id}
    resp = json.load(urllib.urlopen(job_url))
    job_name = resp['job']['name']

    try_daily_job = full_date_re.search(job_name)
    if try_daily_job is not None:
        return datetime.strptime(try_daily_job.group(0), '%Y-%m-%d')

    try_monthly_job = short_date_re.search(job_name)
    if try_monthly_job is not None:
        return datetime.strptime(try_monthly_job.group(0), '%Y-%m') + monthdelta(months=1) - timedelta(days=1)

    # fall back on jub submit date
    job_submit = resp['job']['submitTime']
    return datetime.fromtimestamp(job_submit / 1000.0) - timedelta(days=1)


full_dir_date_re = re.compile('year=(.{2})/month=(.{2})/day=(.{2}).*')
short_dir_date_re = re.compile('year=(.{2})/month=(.{2}).*')


def data_dir_data(dir_path):
    full_date = full_dir_date_re.search(dir_path)
    if full_date is not None:
        return datetime(2000 + int(full_date.group(1)), int(full_date.group(2)), int(full_date.group(3)))

    short_date = short_dir_date_re.search(dir_path)
    if short_date is not None:
        return datetime(2000 + int(short_date.group(1)), int(short_date.group(2)), 1) + monthdelta(months=1) - timedelta(days=1)

    return None


def get_job_inputs(job_id):
    input_conf_name = 'mapreduce.input.fileinputformat.inputdir'
    conf_url = job_conf_endpoint % {'server': job_history_server, 'port': job_history_port, 'job_id': job_id}
    resp = json.load(urllib.urlopen(conf_url))
    configs = resp['conf']['property']
    input_conf = filter(lambda conf: conf['name'] == input_conf_name, configs)
    if len(input_conf) == 0:
        return []
    else:
        return input_conf[0]['value'].split(',')


if __name__ == '__main__':

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-d', '--date', dest='run_date', type=str)
    parser.add_argument('-td', '--time-diff', dest='time_diff', type=int, required=False, default=30)
    parser.add_argument('-o', '--output', dest='output', type=str)

    args = parser.parse_args()

    run_date = datetime.strptime(args.run_date, '%Y-%m-%d')
    all_jobs = get_daily_jobs(run_date, user='airflow')['jobs']['job']
    all_jobs = filter(lambda job: not job['name'].startswith('distcp'), all_jobs)
    print '%d jobs detected' % len(all_jobs)

    diffs = []
    processed = 0
    for job in all_jobs:
        try:
            processed += 1
            print 'at job %d' % processed
            job_date = determine_job_date(job['id'])
            for job_input in get_job_inputs(job['id']):
                input_date = data_dir_data(job_input)
                if input_date is not None:
                    date_diff = job_date - input_date
                    diffs.append([job['name'], job_input, date_diff.days])
        except:
            continue

    diff_df = pd.DataFrame(diffs, columns=['job_name', 'input_dir', 'diff'])
    diff_df[diff_df['diff'] > args.time_diff].sort_values('diff', ascending=False).to_csv(args.output)





