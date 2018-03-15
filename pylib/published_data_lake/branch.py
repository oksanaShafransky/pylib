import abc
import logging


def filter_out_dict(dictionary, keys):
    return {k: v for k, v in dictionary.items() if k not in keys}


logger = logging.getLogger(name='pdl')
logger.setLevel(logging.DEBUG)

PUBLISHED_DATA_LAKE_DB_PREFIX = 'sw_published_'


def db_without_prefix(db):
    return db.split(PUBLISHED_DATA_LAKE_DB_PREFIX)[1]


class BranchableTable(object):
    def __init__(self, db=None, name=None):
        assert db
        assert name
        self.db = db
        self.name = name


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

    def __table_name(self, branchable_table):
        return '{}__{}'.format(branchable_table.name, self.name)

    def __fully_qualified_table_name(self, branchable_table):
        return '{}.{}'.format(branchable_table.db,
                              self.__table_name(branchable_table))

    def __table_location(self, branchable_table):
        return 's3://sw-dag-published-v2/{}/{}/{}'.format(db_without_prefix(branchable_table.db),
                                                          branchable_table.name,
                                                          self.name)

    def partition_location(self, branchable_table, partition):
        return '{}/{}'.format(self.__table_location(branchable_table), partition)