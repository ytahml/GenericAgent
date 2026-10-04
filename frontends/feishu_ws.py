"""GenericAgent's HTTP timeout for Feishu WebSocket endpoint discovery."""
import requests
from lark_oapi.ws import client as ws


_ENDPOINT_TIMEOUT = (10, 30)


class FeishuWsClient(ws.Client):
    def _get_conn_url(self):
        if ws.Strings.is_empty(self._app_id) or ws.Strings.is_empty(self._app_secret):
            raise ws.ClientException(ws.NO_CREDENTIAL, "app_id or app_secret is null")
        headers = {**getattr(self, "_headers", {}), "locale": "zh"}
        if getattr(self, "_user_agent", None):
            headers[ws.USER_AGENT] = self._user_agent
        response = requests.post(
            self._domain + ws.GEN_ENDPOINT_URI,
            headers=headers,
            json={"AppID": self._app_id, "AppSecret": self._app_secret},
            timeout=_ENDPOINT_TIMEOUT,
        )
        if response.status_code != 200:
            raise ws.ServerException(response.status_code, "system busy")
        result = ws.JSON.unmarshal(response.content.decode(ws.UTF_8), ws.EndpointResp)
        if result.code == ws.SYSTEM_BUSY:
            raise ws.ServerException(result.code, "system busy")
        if result.code == ws.INTERNAL_ERROR:
            raise ws.ServerException(result.code, result.msg)
        if result.code != ws.OK:
            raise ws.ClientException(result.code, result.msg)
        if result.data.ClientConfig is not None:
            self._configure(result.data.ClientConfig)
        return result.data.URL
