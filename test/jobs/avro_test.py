__author__ = 'Felix'

import ujson as json
from datetime import *

from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol

from jobs.protocol import TextProtocol
from jobs.builder import Job


def str_now():
    return datetime.now().strftime('%Y-%m-%d %H-%M-%S.%f')


class AvroCountryCount(Job):

    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = TextProtocol

    def mapper(self, index, line):
        datum = json.loads(line)
        yield datum['country'], 1

    def reducer(self, country, counts):

        self.increment_counter('Process', 'Countries', 1)
        yield str(country) + ' ', str(sum(counts))

    def steps(self):
        return [self.mr(mapper=self.mapper, reducer=self.reducer)]

if __name__ == '__main__':
    job = AvroCountryCount()
    job.run()




