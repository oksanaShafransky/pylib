__author__ = 'Felix'


#Defines the api for key/value proxies
class KeyValueProxy:
    def __init__(self):
        pass

    def get(self, key):
        pass

    def set(self, key, value):
        pass

    def delete(self, key):
        pass

    # immediate sub keys only, with relative names
    def sub_keys(self, key):
        pass

    def with_set(self, modified_key, modified_value):
        self.get = lambda key: modified_value if key == modified_key else self.get