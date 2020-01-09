import tejapi
tejapi.ApiConfig.api_key = "your_API_key"
class query_base(object):
    def __init__(self):
        self.tejapi = tejapi
    def set_apikey(self,api_key='yourkey'):
        self.tejapi.ApiConfig.api_key = api_key