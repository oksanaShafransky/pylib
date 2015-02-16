
from snakebite.client import Client
import xml.etree.ElementTree as ET

HADOOP_CONF_DIR = '/etc/hadoop/conf'
hadoop_log_dir_template = '/user/%s/tmp/mrjob/%s/output/_logs/history'


def run(job):
    with job.make_runner() as runner:
        runner.run()

       runner.get_job_name()
       


       job.post_exec(runner=runner)


def get_stats_url(job_name):
    user_id = job_name.split('.')[1]

    nn_host, nn_port = _get_namenode()
    log_url = 'hdfs://%s' % (hadoop_log_dir_template % (user_id, job_name))


def _get_namenode():
    conf = ET.parse('%s/core-site.xml' % HADOOP_CONF_DIR)
    root = conf.getroot()

    # should only be 1
    quorum_prop = [elem.find('value').text for elem in root.findall('property') if elem.find('name').text == 'fs.defaultFS'][0]
    return quorum_prop.split(':')
