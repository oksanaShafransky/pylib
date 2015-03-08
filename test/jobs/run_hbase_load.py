__author__ = 'Felix'

from jobs.builder import JobBuilder
from jobs.run import run
from hbase_load_test import HbaseJob


class TestJob:

    def get_job(self):
        return JobBuilder(job_name='Test HBase Load') \
                              .add_input_path('/user/felix/hbase-load') \
                              .output_to_hbase('happy_felix', cf='try') \
                              .num_reducers(3) \
                              .get_job(HbaseJob)


if __name__ == '__main__':
    job = TestJob().get_job()
    run(job)

