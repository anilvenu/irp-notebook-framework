from .client import Client
from .constants import GET_CURRENCIES, GET_TAGS, CREATE_TAG, GET_MODEL_PROFILES, GET_OUTPUT_PROFILES, GET_EVENT_RATE_SCHEME

class ReferenceDataManager:

    def __init__(self, client: Client):
        self.client = client

    def get_model_profiles(self) -> dict:
        response = self.client.request('GET', GET_MODEL_PROFILES)
        return response.json()

    def get_model_profile_by_name(self, profile_name: str) -> dict:
        params = {'name': profile_name}
        response = self.client.request('GET', GET_MODEL_PROFILES, params=params)
        return response.json()

    def get_output_profiles(self) -> dict:
        response = self.client.request('GET', GET_OUTPUT_PROFILES)
        return response.json()

    def get_output_profile_by_name(self, profile_name: str) -> dict:
        params = {'name': profile_name}
        response = self.client.request('GET', GET_OUTPUT_PROFILES, params=params)
        return response.json()
    
    def get_event_rate_schemes(self) -> dict:
        params = {'where': 'isActive=True'}
        response = self.client.request('GET', GET_EVENT_RATE_SCHEME, params=params)
        return response.json()
    
    def get_event_rate_scheme_by_name(self, scheme_name: str) -> dict:
        params = {'where': f"eventRateSchemeName=\"{scheme_name}\""}
        response = self.client.request('GET', GET_EVENT_RATE_SCHEME, params=params)
        return response.json()

    def get_currencies(self) -> dict:
        params = {
            "fields": "code,name"
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
                tag_ids.append(int(tag_search_response[0]['tagId']))
            else:
                tag_ids.append(int(self.create_tag(tag_name)['id']))

        return tag_ids