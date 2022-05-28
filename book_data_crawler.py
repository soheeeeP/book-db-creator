import json
from urllib import request, parse
from urllib.error import HTTPError


class NaverSearch:
    offset = 1
    default_display = 100

    def __init__(self, display=None, client_id=None, client_secret=None, url=None, key=None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.url = url + key + "="

        if display is None:
            self.display = self.default_display

    def __call__(self, param, offset=None):
        data = self.get_query_response(param)
        if offset is None:
            self.offset += self.display

        return {
            "offset": self.offset,
            "data": data
        }

    def get_query_response(self, param):
        param = parse.quote(param)
        url = self.url + param + "&display=" + str(self.display)
        req = request.Request(url=url)
        req.add_header("X-Naver-Client-Id", self.client_id)
        req.add_header("X-Naver-Client-Secret", self.client_secret)

        try:
            response = request.urlopen(req)
        except HTTPError as e:
            raise Exception('ERROR {}: {}'.format(e.code, e.reason))
        else:
            res_code = response.getcode()

        res_body = response.read().decode('utf-8')
        data = json.loads(res_body)

        return data
