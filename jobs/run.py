
import subprocess
import xml.etree.ElementTree as ET

from builder import user_path
from stats import get_job_stats

HADOOP_CONF_DIR = '/etc/hadoop/conf'
hadoop_user_log_dir_template = '/user/%s/tmp/mrjob/%s/output/_logs/history/'

conf_file_suffix = '_conf.xml'


def run(job):
    with job.make_runner() as runner:
        result = runner.run()

        job_id = None
        log_dir = job.log_dir

        if log_dir == user_path:
            job_name = runner.get_job_name()
            user_id = job_name.split('.')[1]
            log_dir = hadoop_user_log_dir_template % (user_id, job_name)

        if log_dir is not None:
            ls_sp = subprocess.Popen(['hadoop', 'fs', '-ls', log_dir], stdout=subprocess.PIPE)
            history_ls = [line.split(' ')[len(line.split(' '))].replace('\n', '') for line in ls_sp.stdout]
            for file_def in history_ls:
                file = file_def[len(log_dir):]
                if file.endswith(conf_file_suffix):
                    job_id = file[:(len(file) - len(conf_file_suffix))]

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
