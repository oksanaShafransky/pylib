

class AthenaFetcher(object):
    def __init__(self, conn, db=None, max_concurrency=None):
        self._conn = conn
        self._concurrent_reqs = max_concurrency

        self._res_config = {
            'OutputLocation': 's3://similarwebhackathon/'
        }

        self._context = {
            'Database': db or 'default'
        }

    def fetch(self, *queries):
        class QueryIndex:
            def __init__(self, query):
                self.query = query
                self.status = 'NEW'
                self.request = None
                self.result = None

        fetch_status = [QueryIndex(query) for query in queries]

        def _by_status(queries, status):
            return filter(lambda q: q.status == status, queries)

        def _iterate():

            # go over open requests, gather results of finished ones:
            for query_status in _by_status(fetch_status, 'RUNNING'):
                query_info = self._conn.get_query_execution(QueryExecutionId=query_status.request)
                updated_state = query_info['QueryExecution']['Status']['State']
                if updated_state == 'SUCCEEDED':
                    query_status.result = AthenaResultSet(self._conn.get_query_results(QueryExecutionId=query_status.request))
                    query_status.status = 'SUCCEEDED'
                elif updated_state == 'FAILED':
                    query_status.result = None
                    query_status.status = 'FAILED'

            for new_query in _by_status(fetch_status, 'NEW')[len(_by_status(fetch_status, 'RUNNING')) : self._concurrent_reqs]:
                query_exec = self._conn.start_query_execution(QueryString=new_query.query, ResultConfiguration=self._res_config, QueryExecutionContext=self._context)
                new_query.request = query_exec['QueryExecutionId']
                new_query.status = 'RUNNING'

        from time import sleep
        while any(q.status in ['NEW', 'RUNNING'] for q in fetch_status):
            sleep(10)
            _iterate()

        return {q.query: q.result for q in fetch_status}


class AthenaResultSet(object):
    def __init__(self, raw_result):
        if 'ResultSet' not in raw_result:
            raise ValueError('Invalid result')

        self._results = []

        rs = raw_result['ResultSet']
        self._load_columns(rs['ResultSetMetadata'])

        for record in rs['Rows']:
            fields = record['Data']
            record = dict()
            for idx in range(len(self.cols)):
                val = fields[idx].values()[0] if self.cols[idx][1] == 'string' else float(fields[idx].values()[0])
                record[self.cols[idx][0]] = val

            self._results.append(record)

    def _load_columns(self, metadata):
        self.cols = [(col_inf['Name'], col_inf['Type']) for col_inf in metadata['ColumnInfo']]

    @property
    def results(self):
        return self._results

    @property
    def list(self):
        return [rec.values()[0] for rec in self._results]

