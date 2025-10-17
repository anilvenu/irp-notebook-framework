from .client import Client

class TreatyManager:

    def __init__(self, client: Client):
        self.client = client

    def get_treaties_by_edm(self, edm_name: str) -> dict:
        params = {
            "datasource": edm_name,
            "limit": 1000
        }
        response = self.client.request('GET', '/riskmodeler/v1/treaties', params=params)
        return response.json()
    
    def get_treaty_types_by_edm(self, edm_name: str) -> dict:
        params = {
            "fields": "code,name",
            "datasource": edm_name,
            "limit": 1000
        }
        response = self.client.request('GET', '/riskmodeler/v1/domains/RMS/tablespace/System/entities/TreatyType/values', params=params)
        return response.json()
    
    def get_treaty_attachment_bases_by_edm(self, edm_name: str) -> dict:
        params = {
            "fields": "code,name",
            "datasource": edm_name,
            "limit": 1000
        }
        response = self.client.request('GET', '/riskmodeler/v1/domains/RMS/tablespace/System/entities/AttachBasis/values', params=params)
        return response.json()

    def get_treaty_attachment_levels_by_edm(self, edm_name: str) -> dict:
        params = {
            "fields": "code,name",
            "datasource": edm_name,
            "limit": 1000
        }
        response = self.client.request('GET', '/riskmodeler/v1/domains/RMS/tablespace/System/entities/AttachLevel/values', params=params)
        return response.json()

    def create_treaty(self, edm_name: str, treaty_data: dict) -> dict:
        params = {"datasource": edm_name}
        response = self.client.request('POST', '/riskmodeler/v1/treaties', params=params, json=treaty_data)
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
        response = self.client.request('POST', '/riskmodeler/v1/treaties/lob/batch', params=params, json=data)
        return response.json()