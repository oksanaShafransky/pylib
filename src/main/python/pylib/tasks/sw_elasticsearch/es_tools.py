from elasticsearch import Elasticsearch


class ElasticsearchActor(object):
    def __init__(self, ti, index_name):
        self.es = Elasticsearch('http://%s:%d/' % ('es-logstash-ng.service.%s' % ti.env_type, 9200))
        self.current_index = self.ci_creator(ti, index_name)
        self.alias = index_name

    @staticmethod
    def ci_creator(ti, index_name):
        if ti.mode == 'window':
            return "%s_%s" % (index_name, ti.date.strftime('%y_%m_%d'))
        else:
            return "%s_%s" % (index_name, ti.date.strftime('%y_%m'))

    def assert_index_existence(self):
        assert self.es.indices.exists(self.current_index) is True, \
            "Index %s was not found on the server" % self.current_index

    def create_index(self, index_metadata):
        if self.es.indices.exists(self.current_index):
            print("Index '%s' already exists! Deleting..." % self.current_index)
            res = self.es.indices.delete(index=self.current_index)
            print(" response: '%s'" % res)

        print("Creating '%s' index..." % self.current_index)
        res = self.es.indices.create(index=self.current_index, body=index_metadata)
        print(" response: '%s'" % res)

    def update_alias(self):
        if self.es.indices.exists_alias(name=self.alias):
            print("Alias '%s' already exists! Deleting..." % self.alias)
            res = self.es.indices.delete_alias(index="_all", name=self.alias)
            print(" response: '%s'" % res)

        print("Updating alias '%s' to index '%s" % (self.alias, self.current_index))
        res = self.es.indices.put_alias(index=self.current_index, name=self.alias)
        print(" response: '%s'" % res)

    def delete_index(self):
        if self.es.indices.exists(self.current_index):
            print("Deleting '%s' index..." % self.current_index)
            res = self.es.indices.delete(index=self.current_index)
            print(" response: '%s'" % res)
        else:
            print("Index '%s' was not found..." % self.current_index)
