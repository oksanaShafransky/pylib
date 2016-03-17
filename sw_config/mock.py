__author__ = 'Felix'


class Override:
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def invoke(self, fnc):
        return lambda x: self.value if x == self.key else fnc(x)

    def __call__(self, fnc):
        return self.invoke(fnc)


class WithSet:
    def __init__(self, key, value):
        self.modified_key = key
        self.modified_value = value

    @staticmethod
    def invoke(cls, modified_key, new_value):
        class Decorated(cls):
            def __init__(self, *args, **kwargs):
                cls.__init__(self, *args, **kwargs)
                self.get = Override(modified_key, new_value)(self.get)

        return Decorated

    def __call__(self, cls):
        return WithSet.invoke(cls, self.modified_key, self.modified_value)


class WithDelete(WithSet):
    def __init__(self, key):
        self.modified_key = key
        self.modified_value = None

