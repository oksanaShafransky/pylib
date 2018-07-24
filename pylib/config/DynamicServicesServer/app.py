from flask import Flask
from flask import request
from voluptuous import Required, All, Length, Schema, MultipleInvalid
import yaml
import collections
import os
from pylib.config.DynamicServicesServer.ServiceBuilder import ServiceBuilder

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

app = Flask(__name__)
builder = None


schema = Schema({
  Required('service_name'): All(str, Length(min=1)),
  Required('env', default='default'): All(str, Length(min=1))
 })


def convertUnicode(data):
    if isinstance(data, basestring):
        return str(data)
    elif isinstance(data, collections.Mapping):
        return dict(map(convertUnicode, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(convertUnicode, data))
    else:
        return data




@app.route('/serviceResolution')
def get_service():
    try:
        args_dict = convertUnicode(request.args.to_dict())
        args_dict_val = schema(args_dict)
    except MultipleInvalid as e:
        print(str(e))
        return "ERROR - Invalid Arguments: " + str(e)

    env = args_dict_val.get('env')
    service = args_dict_val.get('service_name')
    return builder.getFullService(env, service)


if __name__ == '__main__':
    with open(os.path.join(__location__, 'services_config.yaml'), 'r') as stream:
        try:
            yFile = yaml.load(stream)
        except yaml.   YAMLError as exc:
            print(exc)
    builder = ServiceBuilder(yFile)
    app.run(debug=True, port=12203)