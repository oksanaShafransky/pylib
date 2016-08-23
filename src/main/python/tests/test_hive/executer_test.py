from pylib.hive.hive_exec import Executer
from pylib.tasks.executer import Arg

__author__ = 'Felix'


class PairCalculator(Executer):
    def get_common_params(self):
        first_param = Arg('-lhs', '--left_hand_side', 'left', float, 'First Operand', required=True)
        second_param = Arg('-rhs', '--right_hand_side', 'right', float, 'Second Operand', required=False, default=0.0)

        return [first_param, second_param]


def echo(x):
    print(x)


def add(x, y):
    print(x + y)


def sub(x, y):
    print(x - y)


def mul(x, y):
    print(x * y)


def div(x, y):
    print(x / y)


if __name__ == '__main__':
    calc = PairCalculator()

    calc.add_action('echo', echo, [Arg('-w', '--what', 'what', str, 'Say', required=False, default='hello world')])

    calc.add_action('add', add, [
        calc.common_param('left'),
        calc.common_param('right')
    ])

    calc.add_action('sub', sub, [
        calc.common_param('left'),
        calc.common_param('right')
    ])

    calc.add_action('mul', mul, [
        calc.common_param('left'),
        calc.common_param('right')
    ])

    calc.add_action('div', div, [
        calc.common_param('left'),
        calc.common_param('right')
    ])

    calc.execute()
