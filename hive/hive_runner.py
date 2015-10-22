import subprocess
import re

import tempfile
from urllib2 import urlopen
from datetime import datetime
import time
import socket
import warnings
from xml.sax.saxutils import escape
import os

try:
    from lxml.etree import HTML
    import gelfclient

    CAN_REPORT = True
except ImportError:
    HTML = gelfclient = None
    CAN_REPORT = False


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
        if line.startswith('tracking URL:'):
            tracking_url = line.split(':', 1)[-1]
            conf_url = tracking_url.replace('jobdetails', 'jobconf')
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
    config_xml = '<?xml version="1.0" encoding="UTF-8" standalone="no"?><configuration>'
    config = {}
    config_str = urlopen(conf_url).read()
    config_html = HTML(config_str)
    elements = config_html.xpath('/html/body/table/tbody/tr')
    for element in elements:
        k, v = [i for i in element.itertext() if i != '\n']  # Just two text elements
        config[k] = v
        config_xml += (
            '<property><name>%s</name><value>%s</value><source>dont.know</source></property>' % (escape(k), escape(v)))
    config_xml += '</configuration>'
    return counters, config, config_xml


def run_hive(cmd, log_path=None):
    start_time = datetime.now()
    err_temp = tempfile.TemporaryFile()
    out_temp = tempfile.TemporaryFile()
    p = subprocess.Popen(cmd, stderr=err_temp.fileno(), stdout=out_temp.fileno())
    p.wait()

    err_temp.seek(0)
    out_temp.seek(0)
    stderrdata = err_temp.read()
    stdoutdata = out_temp.read()
    err_temp.close()
    out_temp.close()

    if CAN_REPORT is False:
        warnings.warn('Cannot update db8. Python packages (lxml, gelfclient) missing')
        return
    try:
        end_time = datetime.now()
        hostname = socket.gethostname()

        tmp_path = None
        if not stderrdata:
            return
        for line in stderrdata.split('\n'):
            if 'Hive history file' in line:
                tmp_path = line.split('=', 1)[-1]
                break
        if tmp_path is None:
            return
        log_data = file(log_path, 'rb').read()
        job_ids = re.findall('TASK_HADOOP_ID="(job_\d+_\d+)"', log_data)
        job_ids = list(set(job_ids))

        for job_id in job_ids:
            counters, config, config_xml = get_job_stats(job_id)
            counters_str = '\n'.join(counters)
            counters_dict = dict([line.split('=', 1) for line in counters])
            number_of_input_records = int(counters_dict['Map-Reduce Framework.Map input records'])
            number_of_output_records = int(counters_dict.get('Map-Reduce Framework.Reduce output records', 0))
            number_of_mappers = int(counters_dict['Job Counters.Launched map tasks'])
            number_of_reducers = int(counters_dict.get('Job Counters.Launched reduce tasks', 0))
            job_name = config['mapreduce.job.name']
            mapper_class = config.get('mapred.mapper.class', 'no mapper')
            reducer_class = config.get('mapred.reducer.class', 'no reducer')
            if p.returncode != 0 or counters_dict.get('Job Counters.Failed reduce tasks') or counters_dict.get(
                    'Job Counters.Failed map tasks'):
                job_success = 0
            else:
                job_success = 1
            if number_of_mappers:
                average_mapper_time = int(float(counters_dict[
                                                    'Job Counters.Total time spent by all maps in occupied slots (ms)']) / number_of_mappers / 1000)
            if number_of_reducers:
                average_reducer_time = int(float(counters_dict[
                                                     'Job Counters.Total time spent by all reduces in occupied slots (ms)']) / number_of_reducers / 1000)
            else:
                average_reducer_time = 0
            # # Write to MySQL
            # mysql_cmd = '''INSERT INTO hadoop.job_stats (job_id, job_success, start_time, end_time, job_name, mapper_class, reducer_class, number_of_mappers, number_of_reducers, average_mapper_time, average_reducer_time, submit_host, number_of_input_records, number_of_output_records, counters, config, fail_message)
            #                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            #             '''
            # params = (
            # job_id, job_success, start_time, end_time, job_name, mapper_class, reducer_class, number_of_mappers,
            # number_of_reducers, average_mapper_time, average_reducer_time, hostname, number_of_input_records,
            # number_of_output_records, counters_str, config_xml, None)
            # conn = MySQLdb.connect(host='db8', user='root', passwd='SimilarWeb123',
            #                        db='hadoop')  # TODO: actual password
            # cursor = conn.cursor()
            # cursor.execute(mysql_cmd, params)
            # conn.commit()
            # conn.close()
            #
            # Write to Gelf

            param_dict = {"job_id": job_id,
                          "job_name": job_name,
                          "result": '',
                          "mapper_class": mapper_class,
                          "reducer_class": reducer_class,
                          "start_time": start_time.strftime('%d/%m/%Y %H:%M:%S'),
                          "start_time_ts": time.mktime(start_time.timetuple()) * 1000 + start_time.microsecond / 1000,
                          "end_time": end_time.strftime('%d/%m/%Y %H:%M:%S'),
                          "end_time_ts": time.mktime(start_time.timetuple()) * 1000 + start_time.microsecond / 1000,
                          "num_reducers_set": number_of_reducers,
                          "submit_host": hostname,
                          "failures": 0,
                          "avg_map_time": average_mapper_time,
                          "avg_reduce_time": average_reducer_time,
                          "input_records": number_of_input_records,
                          "output_records": number_of_output_records,
                          "num_mappers": number_of_mappers,
                          "num_reducers": number_of_reducers,
                          "type": "hadoop_hive_mr"}
            for counter_name, counter_value in counters_dict.items():
                param_dict['Counters_%s' % counter_name] = counter_value

            gelf_host = os.environ.get('GELF_HOST', 'localhost')
            gelf = gelfclient.UdpClient(gelf_host)
            gelf.log(param_dict)

    except:
        import traceback

        warnings.warn('Cannot update Kibana. Exception during excecution:\n %s' % traceback.format_exc())
    if p.returncode != 0:
        print 'Hive return code was: %s!' % p.returncode
        print 'Hive stdout: %s' % stdoutdata
        print 'Hive stderr: %s' % stderrdata
        raise subprocess.CalledProcessError(p.returncode, cmd)


def run_hive_job(hql, job_name, num_of_reducers, log_dir, calc_pool='calculation', sync=True, compression='gz'):
    if compression is None or compression == "none":
        compress = "false"
        codec = None
    elif compression == 'gz':
        compress = 'true'
        codec = "org.apache.hadoop.io.compress.GzipCodec"
    elif compression == "bz2":
        compress = 'true'
        codec = "org.apache.hadoop.io.compress.BZip2Codec"
    else:
        raise ValueError('Unknown compression type %s' % compression)

    cmd = ["hive", "-S", "-e", '"%s"' % hql,
           "-hiveconf", "mapreduce.job.name=" + job_name,
           "-hiveconf", "mapreduce.job.reduces=" + str(num_of_reducers),
           "-hiveconf", "mapreduce.job.queuename=" + calc_pool,
           "-hiveconf", "hive.exec.compress.output=" + compress,
           "-hiveconf", "io.seqfile.compression=BLOCK",
           "-hiveconf", "hive.exec.max.dynamic.partitions=100000",
           "-hiveconf", 'hive.log.dir="%s"' % log_dir,
           "-hiveconf", "hive.log.file=hive.log",
           "-hiveconf", "hive.exec.scratchdir=/tmp/hive-prod",
           "-hiveconf", "hive.exec.max.dynamic.partitions.pernode=100000",
           "-hiveconf", "hive.hadoop.supports.splittable.combineinputformat=true",
           "-hiveconf", "mapreduce.input.fileinputformat.split.maxsize=134217728"
           ]
    if codec:
        cmd += ["-hiveconf", "mapreduce.output.fileoutputformat.compress.codec=" + codec]
    if sync:
        return run_hive(cmd, log_path=log_dir + "/hive.log")
    else:
        return subprocess.Popen(cmd)


if __name__ == '__main__':
    run_hive_job('select count(*) from analytics.snapshot_estimated_sr where year=14 and month=05 limit 100',
                 'test job',
                 20)
