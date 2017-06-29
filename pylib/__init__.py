import logging
from logging import config


#################################
#       pylib_log               #
#################################


def is_hadoop_streaming():
    # TODO consider a better way to infer this
    import os
    return 'stream_reduce_output_reader_class' in os.environ or 'stream_map_output_reader_class' in os.environ

# a bit ugly, but for now keeps us from overriding airflow's logger
if len(logging.root.handlers) == 0:
    import os
    curr_path = os.path.dirname(os.path.realpath(__file__)) + '/'
    if not is_hadoop_streaming():
        config.fileConfig(curr_path + 'logging.cfg', disable_existing_loggers=False)
    else:
        config.fileConfig(curr_path + 'streaming_logging.cfg', disable_existing_loggers=False)


class ContextFilter(logging.Filter):
    CURRENT_LOG_NUM = 1

    def filter(self, record):
        record.count = self.CURRENT_LOG_NUM
        self.CURRENT_LOG_NUM += 1
        return True

logging.getLogger('hive').addFilter(ContextFilter())
