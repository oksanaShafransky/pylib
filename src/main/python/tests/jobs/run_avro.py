__author__ = 'Felix'

from pylib.jobs.builder import JobBuilder
from pylib.jobs.run import run
from avro_test import AvroCountryCount


class TestJob:

    def get_job(self):
        return JobBuilder(job_name='Test Mobile Avro') \
                              .add_input_path('/user/felix/avro-mrjob/in') \
                              .with_avro_input() \
                              .output_on('/user/felix/avro-mrjob/out') \
                              .delete_output_on_start() \
                              .with_task_memory(1536) \
                              .num_reducers(23) \
                              .get_job(AvroCountryCount)


if __name__ == '__main__':
    job = TestJob().get_job()
    run(job)

