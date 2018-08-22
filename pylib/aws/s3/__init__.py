import re

s3_url_re = re.compile('(s3[a-z]?://)([^/]+)/(.*)')


def parse_s3_url(s3_url):
    url_match = s3_url_re.match(s3_url)
    return url_match.group(2), url_match.group(3)