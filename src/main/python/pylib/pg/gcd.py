__author__ = 'Felix'


class GCD:
    def __init__(self, a, b, gcd):
        self.a = a
        self.b = b
        self.gcd = gcd


def euclid(x, y):
    s2, s1 = 0, 1
    t2, t1 = 1, 0
    r2, r1 = y, x

    while r2 != 0:
        q = r1 / r2
        r1, r2 = r2, r1 - q * r2
        s1, s2 = s2, s1 - q * s2
        t1, t2 = t2, t1 - q * t2

    return GCD(s1, t1, r1)

