import six

from struct import *


def parse_field(arr, start_idx, data_type):
    if data_type.startswith('list'):
        elem_type = data_type.split(':')[1]
        list_len = unpack('>i', arr[start_idx:start_idx + 4])[0]

        offset = 4
        ret = []
        for i in range(list_len):
            elem_len, elem_value = parse_field(arr, start_idx + offset, elem_type)
            offset += elem_len
            ret += [elem_value]

        return offset, ret

    elif data_type == 'int':
        return 4, unpack('>i', arr[start_idx:start_idx + 4])[0]
    elif data_type == 'short':
        return 2, unpack('>h', arr[start_idx:start_idx + 2])[0]
    elif data_type == 'long':
        return 8, unpack('>q', arr[start_idx:start_idx + 8])[0]
    elif data_type == 'float':
        return 4, unpack('>f', arr[start_idx:start_idx + 4])[0]
    elif data_type == 'double':
        return 8, unpack('>d', arr[start_idx:start_idx + 8])[0]
    elif data_type == 'str' or data_type == 'string':
        str_len = unpack('>h', arr[start_idx:start_idx + 2])[0]
        return 2 + str_len, arr[start_idx + 2:start_idx + 2 + str_len]
    else:
        raise ValueError('Illegal data type passed')


# reads a collection from a binary stream
# it is assumed the stream first encodes collection length and then goes on to encode elements as entailed by the schema
def read_collection(col_stream, col_schema):
    ret = []
    idx = 0

    # length is encoded as integer
    col_len = unpack('>i', col_stream[idx:idx + 4])[0]
    idx += 4

    while len(ret) < col_len:
        next_elem = lambda: None

        for field_name, field_type in col_schema:
            field_len, field_value = parse_field(col_stream, idx, field_type)
            setattr(next_elem, field_name, field_value)
            idx += field_len

        ret += [next_elem]

    return ret


def append_dict(*dicts):
    ret = dict()
    for _dict in dicts:
        for key, val in six.iteritems(_dict):
            ret[key] = val

    return ret


if __name__ == '__main__':

    rank_schema = [('curr_rank', 'int'), ('prev_rank', 'int'), ('app_id', 'str'), ('cover', 'str'), ('title', 'str'), ('author', 'str'), ('price', 'str'), ('rating', 'double'),
                   ('rate_count', 'int'), ('rate_histogram', 'list:int'), ('curr_rating', 'long'), ('curr_rating_count', 'int')]

    import happybase
    conn = happybase.Connection('hbs2-region-a01')

    from datetime import *
    dates = []

    rank_table = conn.table('app_top_list_15_06_14')
    us_ranks = rank_table.row('0  840')
    print(us_ranks.keys())

    free_strm = us_ranks['data:new_free']
    free_rank = read_collection(free_strm, rank_schema)

    free_apps = [app.app_id for app in free_rank]

    paid_strm = us_ranks['data:new_paid']
    paid_rank = read_collection(paid_strm, rank_schema)

    paid_apps = [app.app_id for app in paid_rank]

    app_det_table = conn.table('playstore_app_scraping')
    for app in free_apps + paid_apps:
        app_data = app_det_table.row(app)
        app_date_str = app_data['data:release-date'][2:]
        try:
            app_date = datetime.strptime(app_date_str.strip(), '%B %d, %Y')
            dates += [app_date]
        except:
            pass

    print('dates')
    print(sorted(dates))
