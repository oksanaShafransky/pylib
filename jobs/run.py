
import sys
import re
import xml.etree.ElementTree as ET
from tempfile import *

from mrjob import util

from stats import get_job_stats

HADOOP_CONF_DIR = '/etc/hadoop/conf'
hadoop_user_log_dir_template = '/user/%s/tmp/mrjob/%s/output/_logs/history/'

conf_file_suffix = '_conf.xml'


def run(job):
    out_log = NamedTemporaryFile(delete=False)
    sys.stderr = out_log
    util.log_to_stream(debug=False)

    with job.make_runner() as runner:
        result = runner.run()

        whole_log = ''
        for line in open(out_log.name):
            whole_log += line
            sys.stdout.write(line)

        job_ids = re.findall('job_\d+_\d+', whole_log)

        # for now, assume only one job existed, patch later if needed
        job_id = job_ids[0] if len(job_ids) > 0 else None

        if job_id is not None:
            counters, config, config_xml = get_job_stats(job_id)
            job.counters = dict([line.split('=', 1) for line in counters])
            job.post_exec(result=result, counters=counters, config=config, config_xml=config_xml)
        else:
            job.post_exec(result=result)




def _get_namenode():
    conf = ET.parse('%s/core-site.xml' % HADOOP_CONF_DIR)
    root = conf.getroot()

    # should only be 1
    fs_prop = [elem.find('value').text for elem in root.findall('property') if elem.find('name').text == 'fs.defaultFS'][0]
    return fs_prop[len('hdfs://'):].split(':')
