
def run(job):
   with job.make_runner() as runner:
       runner.run()
       job.post_exec(runner=runner)
