class Purposes(object):
    BigData = 'bigdata'
    WebProduction = 'web-production'
    WebStaging = 'web-staging'
    Ingest = 'ingest'

    @staticmethod
    def get_web_purpose(env):
        return 'web-' + env
