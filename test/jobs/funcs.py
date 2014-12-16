__author__ = 'Felix'


class Summer:

    def __init__(self, factor):
        self.factor = factor

    def apply(self, values):
        sum = 0
        for num in values:
            sum += num

        return sum * self.factor

