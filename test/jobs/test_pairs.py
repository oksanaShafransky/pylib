__author__ = 'Felix'

from jobs.builder import Job

class PairTester(Job):

    def mapper(self, num, line):
        self.increment_counter('Read', 'Line', 1)
        parts = line.split('\t')
        app1 = parts[0]
        app2 = parts[1]
        country = int(parts[2])
        installs, active = float(parts[3]), float(parts[4])

        if installs >= 500.0:
            self.increment_counter('Read', 'Passed', 1)
            yield (app1, app2), str(country)
            yield (app2, app1), str(country)

    def reducer(self, key, values):
        yield key, 'countries: %s' % ','.join(values)

    def steps(self):
        return [self.mr(mapper=self.mapper, reducer=self.reducer)]

if __name__ == '__main__':
    job = PairTester()
    job.run()
