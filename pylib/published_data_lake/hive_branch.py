from pylib.published_data_lake.branch import Branch, BranchableTable
from pyhive import hive
from pyhive.exc import OperationalError


def get_hive_client():
    return hive.Connection(host="10.10.25.91", port=10000)


class HiveBranch(Branch):

    def __init__(self, *args, **kwargs):
        super(HiveBranch, self).__init__(*args, **kwargs)

    def list_branchable_tables(self, dbs):
        client = get_hive_client()
        ans = {}
        for db in dbs:
            branchable_tables_db = []
            branch_tables_db = []
            cursor = client.cursor()
            cursor.execute("SHOW TABLES IN {db} LIKE '*__{branch}'".format(db=db, branch=self.name))
            for result in cursor.fetchall():
                branch_tables_db += [result[0]]
            for branch_table_db in branch_tables_db:
                cursor = client.cursor()
                cursor.execute("SHOW CREATE TABLE {db}.{table}'".format(db=db, table=branch_table_db))
                res = cursor.fetchall()
                res_joined = ' '.join(map(lambda res: res[0], res))
                res_joined_from_location = res_joined.split('LOCATION')[1]
                res_joined_from_root_path = res_joined_from_location.split('//')[1]
                bucket = res_joined_from_root_path.split('/')[0]
                branch_tables_db += [BranchableTable(db=db,
                                                     name=branch_table_db,
                                                     bucket=bucket)]
            ans[db] = branchable_tables_db
        return ans

    def put_partition(self, branchable_table, partition,
                      create_new_table_if_missing=True,
                      use_glue_metastore_reference=True):
        client = get_hive_client()
        table_is_missing=False
        partition_already_exists=False
        table = self._fully_qualified_table_name(branchable_table)
        table_db = branchable_table.db
        partition_sql = ', '.join(["{}='{}'".format(kv.split('=')[0], kv.split('=')[1])
                                   for kv in partition.split('/')])
        try:
            client.cursor()\
            .execute("ALTER TABLE {table} ADD PARTITION ({partition_sql}) location '{location}'"
                     .format(table=table,
                             location=self.partition_location(branchable_table, partition),
                             partition_sql=partition_sql))
            return True
        except OperationalError as operr:
            error_message = operr[0].status.errorMessage
            if 'Table not found' in error_message:
                table_is_missing=True
            elif 'Partition already exists' in error_message:
                partition_already_exists=True
            else:
                raise operr
        if table_is_missing:
            self._create_new_table(branchable_table,
                                   use_glue_metastore_reference=use_glue_metastore_reference)
            return self.put_partition(branchable_table, partition, create_new_table_if_missing=False)
        if partition_already_exists:
            cursor = client.cursor()
            cursor.execute("USE {db}".format(db=table_db))
            cursor.execute("ALTER TABLE {table} PARTITION ({partition_sql}) "
                           "set location '{location}'"
                           .format(table=self._table_name(branchable_table),
                                   location=self.partition_location(branchable_table, partition),
                                   partition_sql=partition_sql))
            return True
        return False

    def _create_new_table(self, branchable_table, use_glue_metastore_reference=False):
        if use_glue_metastore_reference:
            import boto3
            from pylib.published_data_lake.glue_branch import _athena_query
            result = _athena_query('SHOW CREATE TABLE {}'.format(self._fully_qualified_table_name(branchable_table)))
            query_output_location = result['output_location']
            query_output_bucket = query_output_location.replace('s3://','').split('/')[0]
            query_output_path = query_output_location.replace('s3://','').split('/', 1)[1]
            s3 = boto3.resource('s3')
            obj = s3.Object(query_output_bucket, query_output_path)
            create_table_statement = obj.get()['Body'].read().decode('utf-8')
            create_table_statement_without_properties = create_table_statement.split('TBLPROPERTIES')[0]
            get_hive_client().cursor().execute("create DATABASE IF NOT EXISTS {}".format(branchable_table.db))
            get_hive_client().cursor().execute(create_table_statement_without_properties)
        else:
            raise NotImplementedError

    def fork_branch(self, new_branch_name, dbs=None):
        raise NotImplementedError

    def pull_from_branch(self, reference_branch, dbs):
        raise NotImplementedError

    @staticmethod
    def list_dbs():
        raise NotImplementedError
