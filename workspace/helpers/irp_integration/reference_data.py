from .client import Client
from .constants import GET_CURRENCIES, GET_TAGS, CREATE_TAG

class ReferenceDataManager:

    def __init__(self, client: Client):
        self.client = client

    def get_currencies(self) -> dict:
        params = {
            "fields": "code,name",
            "limit": 1000
        }
        response = self.client.request('GET', GET_CURRENCIES, params=params)
        return response.json()
    
    def get_tag_by_name(self, tag_name: str) -> dict:
        params = {
            "isActive": True,
            "filter": f"TAGNAME = '{tag_name}'"
        }
        response = self.client.request('GET', GET_TAGS, params=params)
        return response.json()
    
    def create_tag(self, tag_name: str):
        data = {"tagName": tag_name}
        response = self.client.request('POST', CREATE_TAG, json=data)
        return {"id": response.headers['location'].split('/')[-1]}
    
    def get_tag_ids_from_tag_names(self, tag_names: list) -> list:
        tag_ids = []
        for tag_name in tag_names:
            tag_search_response = self.get_tag_by_name(tag_name)
            if len(tag_search_response) > 0:
                tag_ids.append(tag_search_response[0]['tagId'])
            else:
                tag_ids.append(self.create_tag(tag_name)['id'])

        return tag_ids