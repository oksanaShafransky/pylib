__author__ = 'Felix'

import sys

from time import strftime

from jobs.builder import JobBuilder
from job_test import MyJob


class TestJob:

    def get_job(self):
        return JobBuilder(job_name='TestJob') \
                              .add_input_path('/user/felix/mrjob/sum/in') \
                              .output_to_hbase('happy_felix', cf='try') \
                              .delete_output_on_start() \
                              .include_file('funcs.py') \
                              .set_property('factor', '3') \
                              .num_reducers(3) \
                              .get_job(MyJob)


if __name__ == '__main__':
    job = TestJob().get_job()
    sys.exit(job.execute())