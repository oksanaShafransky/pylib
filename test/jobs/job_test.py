__author__ = 'Felix'

from mrjob.job import MRJob

from funcs import Summer
from jobs.protocol import TsvProtocol, HBaseProtocol
from jobs.builder import Job

SEPARATOR = '\t'

class MyJob(Job):

    OUTPUT_PROTOCOL = HBaseProtocol

    def configure_options(self):
        super(MyJob, self).configure_options()
        self.add_passthrough_option('--factor', help="Multiplicative Factor")

    def mapper(self, _, line):

        fields = line.split(SEPARATOR)
        tok = fields[0]
        count = int(fields[1])

        yield tok, count

    def reducer(self, key, values):

        self.processor = Summer(int(self.options.factor))
        agg_val = self.processor.apply(values)
        hbase_put = {'clean_value': str(agg_val)}
        yield key, hbase_put


    def steps(self):
        return [self.mr(mapper=self.mapper, reducer=self.reducer)]

if __name__ == '__main__':
    job = MyJob()
    job.run()




