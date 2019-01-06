import json
import urllib
from pylib.config.SnowflakeConfig import SnowflakeConfig

RESOURCE_MANAGER_DEFAULT = 'http://%s:8088' % SnowflakeConfig().get_service_name(service_name='active.yarn-rm-mrp')
YARN_APP_ENDPOINT = '%(server)s/ws/v1/cluster/apps'
MAPREDUCE_APP_JOBS_ENDPOINT = '%(track_url)s/ws/v1/mapreduce/jobs'


def get_applications(rm=RESOURCE_MANAGER_DEFAULT, **options):
    request_url = YARN_APP_ENDPOINT % {'server': rm}
    if len(options) > 0:
        request_url += '?%s' % '&'.join(['%s=%s' % (str(param), str(param_val)) for (param, param_val) in options.items()])

    return json.load(urllib.urlopen(request_url))['apps']['app']


def get_application_by_id(app_id, rm=RESOURCE_MANAGER_DEFAULT):
    request_url = '%s/%s' % (YARN_APP_ENDPOINT % {'server': rm}, str(app_id))
    return json.load(urllib.urlopen(request_url))


def get_application_by_params(params):
    request_url = '%s?%s' % (
        YARN_APP_ENDPOINT % {'server': RESOURCE_MANAGER_DEFAULT},
        '&'.join(['%s=%s' % (str(k), str(v)) for (k, v) in params.items()])
    )
    
    resp = json.load(urllib.urlopen(request_url))
    if resp is not None and 'apps' in resp and 'app' in resp['apps']:
       return resp['apps']['app'] 
    else:
        return []


def get_applications_by_tag(app_tag):
    return get_application_by_params({'applicationTags': app_tag})


def get_applications_by_user_and_time(user, start_time, end_time=None):

    params = dict()
    params['user'] = user
    params['startedTimeBegin'] = start_time
    if end_time is not None:
        params['finishedTimeEnd'] = end_time

    return get_application_by_params(params)


def get_app_jobs(application_or_app_id):
    application = application_or_app_id if isinstance(application_or_app_id, dict) \
                                        else get_application_by_id(application_or_app_id)['app']
    track_url = application['trackingUrl']

    request_url = MAPREDUCE_APP_JOBS_ENDPOINT % {'track_url': track_url.rstrip('/')}
    resp = urllib.urlopen(request_url)

    # the tracking url returns a json while the app is still running. afterwards it redirects to a history summary
    # which needs to be parsed
    if application['state'].lower() == 'running':
        return json.load(resp)['jobs']['job']
    else:
        from bs4 import BeautifulSoup
        import re
        content = BeautifulSoup(resp.read(), 'html.parser')
        job_title = content.title.string.strip()
        # wrapping in dict and list to partially comply with json response
        return [{'job_id': re.search('MapReduce Job (job(_[0-9]+)+)', job_title).group(1)}]




