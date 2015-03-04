__author__ = 'Felix'

HASH_DEFAULT = 31


def key_field_partition(target_key, num_partitions, hash_value=HASH_DEFAULT):

    mod_target = target_key % num_partitions
    lookup_table = build_table(num_partitions, hash_value=hash_value)
    return lookup_table[mod_target]


def build_table(num_partitions, hash_value):

    table = {}
    ind = 0
    while len(table) < num_partitions:
        ind_val = sum_by_digits(ind, multiplier=hash_value) % num_partitions
        if ind_val not in table:
            table[ind_val] = ind

        ind += 1

    return table


def sum_by_digits(num, multiplier=1):
    ret = 0
    for digit in str(num):
        ret = ret * multiplier + int(digit)

    return ret
