__author__ = 'Felix'

from mrjob.job import MRJob
from mrjob.protocol import PickleProtocol

from funcs import Summer
from funcs import Token, Count
from jobs.protocol import TsvProtocol, HBaseProtocol
from jobs.builder import Job

class MyJob(Job):

    INPUT_PROTOCOL = TsvProtocol
    INTERNAL_PROTOCOL = PickleProtocol
    OUTPUT_PROTOCOL = HBaseProtocol

    def configure_options(self):
        super(MyJob, self).configure_options()
        self.add_passthrough_option('--factor', help="Multiplicative Factor")

    def mapper(self, key, value):
        self.increment_counter('Read', 'Random Shit', 1)
        yield key, value

    def reducer(self, key, values):

        self.processor = Summer(int(self.options.factor))
        agg_val = self.processor.apply([value.count for value in values])
        hbase_put = {'clean_value': str(agg_val)}
        yield key.token, hbase_put


    def steps(self):
        return [self.mr(mapper=self.mapper, reducer=self.reducer)]

if __name__ == '__main__':
    job = MyJob()
    job.run()




