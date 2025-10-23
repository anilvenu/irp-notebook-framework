from .client import Client
from .constants import GET_TREATIES, CREATE_TREATY, ASSIGN_TREATY_LOBS, GET_TREATY_TYPES, GET_TREATY_ATTACHMENT_BASES, GET_TREATY_ATTACHMENT_LEVELS

class TreatyManager:

    def __init__(self, client: Client):
        self.client = client

    def get_treaties_by_edm(self, edm_name: str) -> dict:
        params = {
            "datasource": edm_name,
            "limit": 100
        }
        response = self.client.request('GET', GET_TREATIES, params=params)
        return response.json()
    
    def get_treaty_types_by_edm(self, edm_name: str) -> dict:
        params = {
            "fields": "code,name",
            "datasource": edm_name,
            "limit": 100
        }
        response = self.client.request('GET', GET_TREATY_TYPES, params=params)
        return response.json()
    
    def get_treaty_attachment_bases_by_edm(self, edm_name: str) -> dict:
        params = {
            "fields": "code,name",
            "datasource": edm_name,
            "limit": 100
        }
        response = self.client.request('GET', GET_TREATY_ATTACHMENT_BASES, params=params)
        return response.json()

    def get_treaty_attachment_levels_by_edm(self, edm_name: str) -> dict:
        params = {
            "fields": "code,name",
            "datasource": edm_name,
            "limit": 100
        }
        response = self.client.request('GET', GET_TREATY_ATTACHMENT_LEVELS, params=params)
        return response.json()

    def create_treaty(self, edm_name: str, treaty_data: dict) -> dict:
        params = {"datasource": edm_name}
        # Truncate treaty number if it exists
        if "treatyNumber" in treaty_data:
            treaty_data["treatyNumber"] = treaty_data["treatyNumber"][:20]
        response = self.client.request('POST', CREATE_TREATY, params=params, json=treaty_data)
        return {'id': response.headers['location'].split('/')[-1]}
    
    def assign_lobs(self, edm_name: str, treaty_id: int, lob_ids: list) -> dict:
        params = {"datasource": edm_name}
        data = []
        for lob_id in lob_ids:
            body_value = {"id": lob_id}
            item = {
                "body": f"{body_value}",
                "method": "POST",
                "path": f"/{treaty_id}/lob"
            }
            data.append(item)
        response = self.client.request('POST', ASSIGN_TREATY_LOBS, params=params, json=data)
        return response.json()