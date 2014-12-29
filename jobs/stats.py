__author__ = 'Felix'

class JobStats:

    def __init__(self, runner):
        self.job_name = runner.get_job_name()
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
        print 'job %s stats:' % stats.job_name

        print stats.counters
