import random


def sanitize(raw_str):
    from six import StringIO
    buff = StringIO()
    for c in raw_str:
        try:
            buff.write(c.decode('ascii'))
        except UnicodeEncodeError and UnicodeDecodeError:
            pass

    return buff.getvalue()


def random_str(length):
    chars = [chr(ord('a') + x) for x in range(ord('z') - ord('a'))]
    return ''.join(random.choice(chars) for i in range(length))