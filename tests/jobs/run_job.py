__author__ = 'Felix'

import sys
from time import strftime

from funcs import Token, Count

from pylib.jobs.builder import JobBuilder
from pylib.jobs.run import run
from job_test import MyJob


class TestJob(object):

    def get_job(self):
        return JobBuilder(job_name='TestJob') \
                              .add_tsv_input_path('/user/felixv/mrjob/sum/in', Token, Count) \
                              .output_to_hbase('happy_felix', cf='try') \
                              .delete_output_on_start() \
                              .include_file('funcs.py') \
                              .set_property('factor', '3') \
                              .num_reducers(3) \
                              .get_job(MyJob)


if __name__ == '__main__':
    job = TestJob().get_job()
    run(job)

