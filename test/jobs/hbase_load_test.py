__author__ = 'Felix'

from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol

from jobs.protocol import TsvProtocol, HBaseProtocol
from jobs.builder import Job

class HbaseJob(Job):

    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = HBaseProtocol

    def mapper(self, index, line):
        key, value = line.split(' ')
        yield key, value

    def reducer(self, key, values):

        for val in values:
            hbase_put = {'prompt': val}
            yield key, hbase_put


    def steps(self):
        return [self.mr(mapper=self.mapper, reducer=self.reducer)]

if __name__ == '__main__':
    job = HbaseJob()
    job.run()




