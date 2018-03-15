from pylib.published_data_lake.branch import Branch
from pylib.published_data_lake.glue_branch import GlueBranch


class GlueHiveBranch(Branch):

    def __init__(self, *args, **kwargs):
        super(GlueHiveBranch, self).__init__(*args, **kwargs)
        self._glue_branch = GlueBranch.__init__(*args, **kwargs)

    def list_branchable_tables(self, dbs):
        return self._glue_branch.list_branchable_tables(dbs)

    def put_partition(self, branchable_table, partition, create_new_table_if_missing=True):
        return self._glue_branch.put_partition(branchable_table, partition, create_new_table_if_missing=create_new_table_if_missing)

    def fork_branch(self, new_branch_name, dbs=None):
        return self._glue_branch.fork_branch(new_branch_name, dbs=dbs)

    def pull_from_branch(self, reference_branch, dbs):
        return self._glue_branch.pull_from_branch(reference_branch, dbs)

    @staticmethod
    def list_dbs():
        return GlueBranch.list_dbs()
