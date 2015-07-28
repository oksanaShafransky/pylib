__author__ = 'Felix'


def sanitize(raw_str):
    from StringIO import StringIO
    buff = StringIO()
    for c in raw_str:
        try:
            buff.write(c.decode('ascii'))
        except UnicodeEncodeError:
            pass

    return buff.getvalue()
