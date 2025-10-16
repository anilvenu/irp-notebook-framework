from .client import Client

class ReferenceDataManager:

    def __init__(self, client: Client):
        self.client = client

    def get_currencies(self) -> dict:
        params = {
            "fields": "code, name",
            "limit": 1000
        }
        response = self.client.request('GET', '/riskmodeler/v1/domains/Client/tablespace/UserConfig/entities/currency/values')
        return response.json()