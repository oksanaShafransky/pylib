__author__ = 'Felix'

class JobStats:

    def __init__(self, runner):
        self.counters = runner.counters()
        self.opts = runner.get_opts()


class PostJobHandler:

    def __init__(self, recorders):
        self.recorders = recorders

    def handle_job(self, **kwargs):

        runner = kwargs['runner']
        stats = JobStats(runner)

        for recorder in self.recorders:
            recorder.record(stats)


class PrintRecorder:

    def record(self, stats):
        print 'got %d counters' % len(stats.counters)

        print stats.counters

        print 'these are the opts:'
        print stats.opts
