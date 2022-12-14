from elasticsearch import Elasticsearch

class ElasticsearchActor(object):
    def __init__(self, index_name, date_suffix, es_uri, alias=""):
        """
        :param index_name:
        :param date_suffix:
        :param alias: alias name for update (defaults index name)
        :param es_uri: elasticsearch base url (e.g. http://host:9200)
        :return:
        """
        self.es = Elasticsearch(es_uri)
        self.current_index = '%s_%s' % (index_name, date_suffix)
        self.alias = index_name if alias == "" else alias

    def assert_index_existence(self):
        assert self.es.indices.exists(self.current_index) is True, \
            "Index %s was not found on the server" % self.current_index

    def assert_index_doc_count(self, min_documents=1000):
        self.assert_index_existence()
        docs = self.es.indices.stats(index=self.current_index, metric='docs')['_all']['primaries']['docs']['count']
        assert docs > min_documents, "Index %s doesn't have enough documents (%d < %d)" % (self.current_index, docs, min_documents)

    def create_index(self, index_metadata):
        if self.es.indices.exists(self.current_index):
            print("Index '%s' already exists! Deleting..." % self.current_index)
            res = self.es.indices.delete(index=self.current_index)
            print(" response: '%s'" % res)

        print("Creating '%s' index..." % self.current_index)
        res = self.es.indices.create(index=self.current_index, body=index_metadata)
        print(" response: '%s'" % res)

    def update_alias(self, ignore_date=False):
        if self.es.indices.exists_alias(name=self.alias):
            aliased_index_name = self.es.indices.get_alias(self.alias).keys()[0]

            #comparing strings to verify new index is most recent
            if not ignore_date:
                assert self.current_index > aliased_index_name, \
                    "more recent index (%s < %s) already exists! no update is required" % (self.current_index, aliased_index_name)

            print("Alias already exists for index '%s' Deleting..." % aliased_index_name)
            res = self.es.indices.delete_alias(index="_all", name=self.alias)
            print(" response: '%s'" % res)

        print("Updating alias '%s' to index '%s" % (self.alias, self.current_index))
        res = self.es.indices.put_alias(index=self.current_index, name=self.alias)
        print(" response: '%s'" % res)

    def delete_index(self, ignore_alias=False):
        if self.es.indices.exists(self.current_index):
            if self.es.indices.exists_alias(name=self.alias):
                aliased_index_name = self.es.indices.get_alias(self.alias).keys()[0]
                # comparing strings to verify deleted index is not aliased
                if not ignore_alias:
                    assert self.current_index != aliased_index_name, \
                        "Index %s is aliased! Avoiding deletion" % aliased_index_name
            print("Deleting '%s' index..." % self.current_index)
            res = self.es.indices.delete(index=self.current_index)
            print(" response: '%s'" % res)
        else:
            print("Index '%s' was not found..." % self.current_index)

