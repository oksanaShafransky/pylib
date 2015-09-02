# f = file('c:/temp/window.txt')
# for line in f:
#     args = line.split()
#     current_mode = None
# 
#     task = {}
#     for arg in args:
#         if arg.startswith('-'):
#             current_mode = arg
#             task[arg] = []
#         elif current_mode:
#             task[current_mode].append(arg)
# 
#     task_name = args[0].rstrip('_')
#     if '-e' not in task:
#         print args
#         continue
#     code = '''%(task_name)s = DockerBashOperator(
#     task_id='%(task_name)s',
#     dag=dag,
#     docker_name="op-hbs2",
#     bash_command='{{ params.execution_dir }}/%(script)s -d {{ ds }} -m %(mode)s -mt %(mode_type)s %(parts)s'
# )
#     ''' % {'task_name': task_name,
#            'script': task['-e'][0],
#            'mode': task.get('-m','window'),
#            'mode_type': task.get('-mt','last-28'),
#            'parts': '-p ' + task['-p'][0] if task.get('-p') else ''}
#     deps = ''
#     for dep in task.get('-d',[]):
#         deps += '%(task_name)s.set_upstream(%(dep_name)s)\n' %{'task_name': task_name, 'dep_name': dep.rstrip('_')}
#     print code
#     print deps

from airflow.operators.dummy_operator import DummyOperator
from operators import DockerBashOperator, CopyHbaseTableOperator, DockerBashSensor



# Test for deciding when to start
import sys, os
sys.path += os.path.abspath('.')
from moving_window.daily_calculation import *
from moving_window.window_calculation import *
from moving_window.deployment import *

