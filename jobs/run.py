
hadoop_log_dir_template = '/user/%s/tmp/mrjob/%s/output/_logs/history'

def run(job):
    with job.make_runner() as runner:
        runner.run()

       runner.get_job_name()



       job.post_exec(runner=runner)


def get_stats_url(job_name):
    user_id = job_name.split('.')[1]
    log_url = 'hdfs://%s' % (hadoop_log_dir_template % (user_id, job_name))
