__author__ = 'Felix'

from struct import *


def is_prime(n):
    if n < 2:
        return False
    for p in range(2, int(n ** 0.5) + 1):
        if n % p == 0:
            return False

    return True


def gen_small_primes(max_num):
    n = 2
    while n <= max_num:
        if is_prime(n):
            yield n
        n += 1


def write_to_file(nums, f):
    writer = open(f, 'w')

    for num in nums:
        writer.write(pack('>Q', num))

    writer.close()


def load_primes(f, amount=None):
    reader = open(f, 'rb')
    buff = reader.read() if amount is None else reader.read(8 * amount)
    reader.close()
    return [unpack('>Q', buff[8 * i:8 * (i + 1)])[0] for i in range(len(buff) / 8)]


if __name__ == '__main__':
    #write_to_file(gen_small_primes(2 ** 32), 'c:/tmp/primes.bin')
    primes = load_primes('c:/tmp/primes.bin')
    print len(primes)
    print primes[:100]