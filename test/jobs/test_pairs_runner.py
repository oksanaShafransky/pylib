__author__ = 'Felix'

from jobs.builder import JobBuilder
from jobs.run import run

from test_pairs import PairTester

if __name__ == '__main__':

    job = JobBuilder(job_name='Filter App Pairs') \
          .input_text_from('/similargroup/data/mobile-analytics/daily/aggregate/aggkey=AppPairCountryKey/year=14/month=06/day=01') \
          .output_on('/user/felix/app-pairs') \
          .delete_output_on_start() \
          .num_reducers(5) \
          .get_job(PairTester)

    run(job)
