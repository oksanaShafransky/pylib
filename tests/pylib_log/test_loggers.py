import logging
import sys
import pytest


class MessageAggregator(object):

    def __init__(self):
        self.messages = []

    def tell(self, msg):
        self.messages.append(msg)


class TestLog(object):

    @pytest.fixture(scope='function')
    def handler(self):
        return MessageAggregator()

    # def test_main_log_level(self, handler, monkeypatch):
    #     logger = logging.getLogger()
    #     monkeypatch.setattr(sys.stdout, 'write', handler.tell)
    #     logger.info('hello1')
    #     logger.debug('hello2')  # should be ignored because main log level is expected to be info+
    #
    #     assert len(handler.messages) == 1

    def test_hive_log_level(self, handler, monkeypatch):
        logger = logging.getLogger('hive')
        monkeypatch.setattr(sys.stdout, 'write', handler.tell)

        # this time around, both will be written, as hive logger is set to debug
        logger.info('hello1')
        logger.debug('hello2')

        assert len(handler.messages) == 2

    def test_hive_log_counter(self, handler, monkeypatch):
        logger = logging.getLogger('hive')
        monkeypatch.setattr(sys.stdout, 'write', handler.tell)

        logger.info('first message')
        logger.info('second message')
        logger.info('third message')

        import re
        hive_logger_re = re.compile('.*\[.*: ([0-9]+) \]')
        counts = [int(hive_logger_re.match(msg).group(1)) for msg in handler.messages]

        def is_num_seq(lst, seq_len):
            return map(lambda pair: pair[1] - pair[0], zip([lst[0] for _ in lst], lst)) == range(seq_len)

        # since previous messages where written to hte log during test, cant assume they will be 1,2,3. x,x+1,x+2 works
        assert is_num_seq(counts, 3)


