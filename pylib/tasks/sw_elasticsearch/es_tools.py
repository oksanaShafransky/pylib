from elasticsearch import Elasticsearch


class ElasticsearchActor(object):
    def __init__(self, mode, export_date, index_name, es_uri):
        """
        :param mode: window/snapshot
        :param export_date: date to generate
        :param index_name:
        :param es_uri: elasticsearch base url (e.g. http://host:9200)
        :return:
        """
        self.es = Elasticsearch(es_uri)
        self.current_index = self.ci_creator(mode=mode, export_date=export_date, index_name=index_name)
        self.alias = index_name

    @staticmethod
    def ci_creator(mode, export_date, index_name):
        if mode == 'window':
            return "%s_%s" % (index_name, export_date.strftime('%y_%m_%d'))
        else:
            return "%s_%s" % (index_name, export_date.strftime('%y_%m'))

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
