__author__ = 'Felix'


class Summer:

    def __init__(self, factor):
        self.factor = factor

    def apply(self, values):
        sum = 0
        for num in values:
            sum += num

        return sum * self.factor

class Token:

    def __init__(self):
        self.token = ''

    def read_tsv(self, fields, idx):
        self.token = fields[idx]
        return idx + 1

    def to_tsv(self):
        return self.token


class Count:

    def __init__(self):
        self.count = 0

    def read_tsv(self, fields, idx):
        self.count = int(fields[idx])
        return idx + 1

    def to_tsv(self):

        return str(self.count)
