__author__ = 'Felix'

import random
from nanotime import *

from gcd import euclid
from small_prime import *

primes_to_use = \
{
    32 * 8: 10000,
    64 * 8: 20000,
    128 * 8: 60000,
    256 * 8: 150000,
    512 * 8: 10000000
}


def decompose(n):
    two_exp = 0

    while n % 2 == 0:
        n /= 2
        two_exp += 1

    return two_exp, n


def is_witness(possible_witness, p, exponent, remainder):
    possible_witness = pow(possible_witness, remainder, p)

    if possible_witness == 1 or possible_witness == p - 1:
        return False

    curr_idx = 1
    curr_value = possible_witness
    while curr_idx < exponent:
        curr_value = pow(curr_value, 2, p)
        curr_idx += 1
        if curr_value == 1:
            return True
        if curr_value == p - 1:
            return False

    return True


def is_prime(p, num_witnesses=2):
    if p == 2 or p == 3:
        return True
    elif p < 2:
        return False

    exponent, remainder = decompose(p - 1)

    if is_witness(2, p, exponent, remainder):
            return False

    for _ in range(num_witnesses):
        witness = random.randint(2, p - 2)
        if is_witness(witness, p, exponent, remainder):
            return False

    return True


def sieve(num, vicinity, small_primes, jump=1):
    status = [True for _ in range(vicinity)]
    for pi in small_primes:
        gcd = euclid(jump, pi)
        div_idx = (-gcd.a * num) % pi
        while div_idx < vicinity:
            status[div_idx] = False
            div_idx += pi

    return [num + idx * jump for idx in range(vicinity) if status[idx]]


def generate_prime(bit_size, small_primes, search_size=None):
    if search_size is None:
        search_size = bit_size * 3
    print('search size is %d' % search_size)
    while True:
        n = random.randint(2 ** (bit_size - 1), (2 ** bit_size - 1))
        if n % 2 == 0:
            n += 1
        candidates = sieve(n, search_size / 2, small_primes, 2)
        print('%d candidates after sieving' % len(candidates))
        attempts = 0
        while len(candidates) > 0:
            attempts += 1
            next_cand = candidates[random.randint(0, len(candidates) - 1)]
            if is_prime(next_cand):
                print('succeeded at %d attempt' % attempts)
                return next_cand
            else:
               candidates.remove(next_cand)
        print('no prime among candidates')


def generate_germain_prime(bit_size, small_primes, search_size=None):
    if search_size is None:
        search_size = bit_size * bit_size * 12
    print('search size is %d' % search_size)
    while True:
        n = random.randint(2 ** (bit_size - 1), (2 ** bit_size - 1))
        if n % 2 == 0:
            n += 1
        print(nanotime.now())
        candidates_q = set(sieve(n, search_size / 2, small_primes, 2))
        candidates_p = set([(p - 1) / 2 for p in sieve(n * 2 + 1, search_size / 2, small_primes, 4)])
        candidates = list(candidates_p.intersection(candidates_q))
        print('%d candidates after sieving' % len(candidates))
        attempts = 0
        while len(candidates) > 0:
            attempts += 1
            next_cand = candidates[random.randint(0, len(candidates) - 1)]
            if is_prime(next_cand):
                print(nanotime.now())
                print('prime q at %d attempt' % attempts)
                if is_prime(next_cand * 2 + 1):
                    print('prime p at %d attempt!!!' % attempts)
                    return next_cand

            candidates.remove(next_cand)

        print('no prime among candidates')


def test_primality_test_performance(num_size, num_runs):
    start_time = nanotime.now().nanoseconds()

    for _ in range(num_runs):
        rand_p = random.randint(2 ** (num_size - 1), (2 ** num_size) - 1)
        if rand_p % 2 == 0:
            rand_p += 1
        is_prime(rand_p)

    end_time = nanotime.now().nanoseconds()
    avg_run_time = (end_time - start_time) / num_runs

    print('on average, miller-rabin took %.5f milliseconds' % (avg_run_time / 1000000.0))


def test_prime_gen(num_size, num_runs):
    start_time = nanotime.now().nanoseconds()
    small_primes = [p for p in load_primes('c:/tmp/primes.bin')]
    found = 0
    for _ in range(num_runs):
        p = generate_prime(num_size, small_primes[:primes_to_use[num_size]])
        found += 1
        print('%d: found prime %d' % (found, p))

    end_time = nanotime.now().nanoseconds()
    avg_run_time = (end_time - start_time) / num_runs

    print('on average, prime generation took %.5f seconds' % (avg_run_time / 1000000000.0))


def test_germain_gen(num_size, num_runs):
    start_time = nanotime.now().nanoseconds()
    small_primes = [p for p in load_primes('c:/tmp/primes.bin')]
    found = 0
    for _ in range(num_runs):
        p = generate_germain_prime(num_size, small_primes[:primes_to_use[num_size]])
        found += 1
        print('%d: %d found' % (found, p))

    end_time = nanotime.now().nanoseconds()
    avg_run_time = (end_time - start_time) / num_runs

    print('on average, prime generation took %.5f seconds' % (avg_run_time / 1000000000.0))


if __name__ == '__main__':
    #test_primality_test_performance(512 * 8, 100)
    #print sieve(150, 100, [2, 3, 5, 7])
    #test_prime_gen(512 * 8, 100)
    test_germain_gen(512 * 8, 10)
