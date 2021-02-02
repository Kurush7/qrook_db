# todo make hair from qrequest
class QRSelect:
    connector = None
    request= None

    # result of base select
    def __init__(self, connector=None, request=None, params=None):
        self.connector = connector
        self.request = request
        self.params = params

    # todo add layer with data restructing
    def all(self):
        sql = self.request.format(*self.params)
        data = self.connector.select(sql)
        return data


    # todo interesting, but wtf?
    def __call__(self, *args, **kwargs):
        pass
