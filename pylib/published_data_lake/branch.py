import abc
import logging


def filter_out_dict(dictionary, keys):
    return {k: v for k, v in dictionary.items() if k not in keys}


logger = logging.getLogger(name='pdl')
logger.setLevel(logging.DEBUG)

PUBLISHED_DATA_LAKE_DB_PREFIX = 'sw_published_'


class BranchableTable(object):
    def __init__(self, db=None, name=None, bucket=None):
        assert db
        assert db.startswith(PUBLISHED_DATA_LAKE_DB_PREFIX)
        assert name
        assert bucket
        self.db = db
        self.name = name
        self.bucket = bucket


class Branch(object):

    def __init__(self, name=None):
        assert name
        self.name = name

    @abc.abstractmethod
    def list_branchable_tables(self, dbs):
        pass

    @abc.abstractmethod
    def put_partition(self, branchable_table, partition, create_new_table_if_missing=True):
        pass

    @abc.abstractmethod
    def fork_branch(self, new_branch_name, dbs=None):
        pass

    @abc.abstractmethod
    def pull_from_branch(self, reference_branch, dbs):
        return

    @staticmethod
    @abc.abstractmethod
    def list_dbs():
        return

    def _table_name(self, branchable_table):
        return '{}__{}'.format(branchable_table.name, self.name)

    def _fully_qualified_table_name(self, branchable_table):
        return '{}.{}'.format(branchable_table.db,
                              self._table_name(branchable_table))

    def _table_location(self, branchable_table, fs='s3'):
        return '{fs}://{bucket}/{db}/{branchable_table}/{branch}'.format(fs=fs,
                                                                            bucket=branchable_table.bucket,
                                                                            db=branchable_table.db,
                                                                            branchable_table=branchable_table.name,
                                                                            branch=self.name)

    def partition_location(self, branchable_table, partition):
        return '{}/{}'.format(self._table_location(branchable_table), partition)