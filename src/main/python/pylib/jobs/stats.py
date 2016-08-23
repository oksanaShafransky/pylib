__author__ = 'Felix'

import subprocess
from six.moves.urllib.request import urlopen
from xml.sax.saxutils import escape


def check_output(*popenargs, **kwargs):
    """Run command with arguments and return its output as a byte string.
    Backported from Python 2.7 as it's implemented as pure python on stdlib.
    """
    process = subprocess.Popen(stdout=subprocess.PIPE, *popenargs, **kwargs)
    output, unused_err = process.communicate()
    retcode = process.poll()
    if retcode:
        cmd = kwargs.get("args")
        if cmd is None:
            cmd = popenargs[0]
        error = subprocess.CalledProcessError(retcode, cmd)
        error.output = output
        raise error
    return output


def get_job_stats(job_id):

    cmd = 'mapred job -status %s' % job_id
    output = check_output(cmd, shell=True)
    conf_url = None
    # Get counters and tracking url
    counters_started = False
    current_counter_head = ''
    counters = []
    for line in output.split('\n'):
        if line.startswith('Job Tracking URL'):
            tracking_url = line.split(':', 1)[-1]
        if line.startswith('Job File'):
            conf_as_text_cmd = 'hadoop fs -text %s' % line.split(':', 1)[-1]
        if line.startswith('Counters: '):
            counters_started = True
            continue
        if not counters_started:
            continue
        if not '=' in line:
            current_counter_head = line.strip()
        else:
            counters.append('%s.%s' % (current_counter_head, line.strip()))

    # Get config
    config_xml = check_output(conf_as_text_cmd, shell=True)
    config = parse_config_xml(config_xml)

    return counters, config, config_xml


def parse_config_xml(config_xml):
    ret = {}

    from xml.etree import ElementTree as ET
    config_doc = ET.fromstring(config_xml)

    properties = config_doc.findall('./property')
    for prop in properties:
        key, value = None, None
        for elem in prop:
            if elem.tag == 'name':
                key = elem.text
            elif elem.tag == 'value':
                value = elem.text

            if key is not None and value is not None:
                ret[key] = value    

    return ret


class JobStats:

    def __init__(self, **kwargs):
        self.run_result = kwargs['result']
        config = kwargs['config']
        self.config_xml = kwargs['config_xml']
        counters = kwargs['counters']

        self.job_name = config['mapreduce.job.name']
        self.mapper_class = config.get('mapred.mapper.class', 'no mapper')
        self.reducer_class = config.get('mapred.reducer.class', 'no reducer')

        self.counters_str = '\n'.join(counters)
        self.counters = dict([line.split('=', 1) for line in counters])

        self.parse_counters()

    def parse_counters(self):
        self.job_success = 0 if self.run_result != 0 or self.counters.get('Job Counters.Failed reduce tasks') or \
        self.counters.get('Job Counters.Failed map tasks') else 1

        self.number_of_input_records = int(self.counters['Map-Reduce Framework.Map input records'])
        self.number_of_output_records = int(self.counters.get('Map-Reduce Framework.Reduce output records', 0))
        self.number_of_mappers = int(self.counters['Job Counters.Launched map tasks'])
        self.number_of_reducers = int(self.counters.get('Job Counters.Launched reduce tasks', 0))

        self.average_mapper_time = int(float(self.counters['Job Counters.Total time spent by all maps in occupied slots (ms)']) / self.number_of_mappers / 1000)
        self.average_reducer_time = int(float(self.counters['Job Counters.Total time spent by all reduces in occupied slots (ms)']) / self.number_of_reducers / 1000) \
            if self.number_of_reducers else 0


class PostJobHandler:

    def __init__(self, recorders):
        self.recorders = recorders

    def handle_job(self, **kwargs):

        stats = JobStats(**kwargs)

        for recorder in self.recorders:
            recorder.record(stats)


class PrintRecorder:

    def record(self, stats):
        print('job %s stats:' % stats.job_name)

        print(stats.counters)
