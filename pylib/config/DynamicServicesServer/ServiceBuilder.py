

class ServiceBuilder:

    outputStringTemplate = "{service_name}-{cluster}.service.{data_center}.consul"

    def __init__(self, yFile):
        self.yFile = yFile

    def getFullService(self, env, service):
        current_env = self.yFile.get(env)
        service_name = self.yFile.get("services_names").get(service)
        data_center = current_env.get("data_center")
        cluster = current_env.get("cluster")
        params = {'service_name': service_name,
                  'data_center': data_center,
                  'cluster': cluster}
        return self.outputStringTemplate.format(**params)
