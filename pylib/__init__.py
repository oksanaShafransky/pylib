import sys
import logging
from logging import config
import pkg_resources


#################################
#       pylib_log               #
#################################


def is_hadoop_streaming():
    # TODO consider a better way to infer this
    import os
    return 'stream_reduce_output_reader_class' in os.environ or 'stream_map_output_reader_class' in os.environ


# a bit ugly, but for now keeps us from overriding airflow's logger
logger_preserving_modules = ['airflow']
if not any([mod in sys.modules for mod in logger_preserving_modules]):
    import os
    curr_path = os.path.dirname(os.path.realpath(__file__)) + '/'
    if not is_hadoop_streaming():
        config.fileConfig(pkg_resources.resource_stream('.'.join([__package__, 'resources', 'logging']), 'logging.cfg'),
                          disable_existing_loggers=False)
    else:
        # hadoop streaming relies on stdout to pass records, so need to ensure no logs are written there
        config.fileConfig(pkg_resources.resource_stream('.'.join([__package__, 'resources', 'logging']),
                                                        'streaming_logging.cfg'), disable_existing_loggers=False)


class ContextFilter(logging.Filter):
    CURRENT_LOG_NUM = 1

    def filter(self, record):
        record.count = self.CURRENT_LOG_NUM
        self.CURRENT_LOG_NUM += 1
        return True

logging.getLogger('hive').addFilter(ContextFilter())
