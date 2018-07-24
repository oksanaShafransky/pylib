

class ServiceBuilder:

    current_env = None
    outputStringTemplate = "{service_name}-{cluster}.service.{data_center}.consul"

    def __init__(self, yFile):
        self.yFile = yFile

    def __set_env(self,env):
        self.current_env = self.yFile.get(env)

    def getFullService(self, env, service):
        self.__set_env(env)
        service_name = self.yFile.get("services_names").get(service)
        data_center = self.current_env.get("data_center")
        cluster = self.current_env.get("cluster")
        params = {'service_name': service_name,
                  'data_center': data_center,
                  'cluster': cluster}
        return self.outputStringTemplate.format(**params)
