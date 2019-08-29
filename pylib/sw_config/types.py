class Purposes(object):
    BigData = 'bigdata'
    WebProduction = 'web-production'
    WebStaging = 'web-staging'
    Ingest = 'ingest'
    Local = 'local'

    @staticmethod
    def get_web_purpose(env):
        if env.lower() == 'production':
            return Purposes.WebProduction
        elif env.lower() == 'staging':
            return Purposes.WebStaging
        else:
            raise KeyError('Invalid web env: ' + env)
