__author__ = 'Felix'


def sanitize(raw_str):
    from six import StringIO
    buff = StringIO()
    for c in raw_str:
        try:
            buff.write(c.decode('ascii'))
        except UnicodeEncodeError and UnicodeDecodeError:
            pass

    return buff.getvalue()