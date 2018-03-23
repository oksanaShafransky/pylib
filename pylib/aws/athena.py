

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
        queries_left = list(queries)
        open_requests = dict()
        results = dict()

        def _iterate():

            # go over open requests, gather results of finished ones:
            handled = list()
            for request, query in open_requests.items():
                query_info = self._conn.get_query_execution(QueryExecutionId=request)
                if query_info['QueryExecution']['Status']['State'] == 'SUCCEEDED':
                    results[query] = AthenaResultSet(self._conn.get_query_results(QueryExecutionId=request))
                    handled.append(request)
                    queries_left.remove(query)

            for cleared in handled:
                open_requests.pop(cleared)

            for query in queries_left:
                if self._concurrent_reqs is not None and len(open_requests) > self._concurrent_reqs:
                    break

                if query not in results.keys():
                    query_exec = self._conn.start_query_execution(
                        QueryString=query, ResultConfiguration=self._res_config, QueryExecutionContext=self._context)
                    open_requests[query_exec['QueryExecutionId']] = query

        from time import sleep
        while len(queries_left) > 0:
            sleep(10)
            _iterate()

        return results


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

