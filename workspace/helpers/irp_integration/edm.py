from .client import Client
from .constants import GET_DATASOURCES, CREATE_DATASOURCE, DELETE_DATASOURCE, EXPORT_EDM, GET_CEDANTS, GET_LOBS

class EDMManager:
    def __init__(self, client: Client, portfolio_manager=None):
        self.client = client
        self._portfolio_manager = portfolio_manager

    @property
    def portfolio_manager(self):
        if self._portfolio_manager is None:
            # Lazy import to avoid circular dependencies
            from .portfolio import PortfolioManager
            self._portfolio_manager = PortfolioManager(self.client)
        return self._portfolio_manager

    def get_all_edms(self) -> dict:
        response = self.client.request('GET', GET_DATASOURCES)
        return response.json()
    
    def get_edm_by_name(self, edm_name: str) -> dict:
        params = {"q": f"datasourceName={edm_name}"}
        response = self.client.request('GET', GET_DATASOURCES, params=params)
        return response.json()

    def create_edm(self, edm_name: str, server_name: str) -> dict:
        params = {
            "datasourcename": edm_name,
            "servername": server_name,
            "operation": "CREATE"
        }
        response = self.client.execute_workflow('POST', CREATE_DATASOURCE, params=params)
        return response.json()
    
    def duplicate_edm(self, source_edm_name: str, dest_edm_name: str = "") -> dict:
        if not dest_edm_name:
            dest_edm_name = f"np_{source_edm_name}"
        
        portfolios_response = self.portfolio_manager.get_portfolios_by_edm_name(source_edm_name)
        portfolio_ids = []
        for portfolio in portfolios_response['searchItems']:
            portfolio_ids.append(portfolio['id'])

        data = {
            "createnew": True,
            "exportType": "EDM",
            "sourceDatasource": source_edm_name,
            "destinationDatasource": dest_edm_name,
            "exposureType": "PORTFOLIO",
            "exposureIds": portfolio_ids,
            "download": False,
            "exportFormat": "BAK",
            "exportOptions": {
                "exportAccounts": True,
                "exportLocations": True,
                "exportPerilDetailsInfo": True,
                "exportPolicies": True,
                "exportReinsuranceInfo": True,
                "exportTreaties": True
            },
            "preserveIds": True,
            "sqlVersion": 2019,
            "type": "ExposureExportInput"
        }

        response = self.client.execute_workflow('POST', EXPORT_EDM, json=data)
        return response.json()
    
    def upgrade_edm_version(self, edm_name: str) -> dict:
        params = {
            "datasourcename": edm_name,
            "operation": "EDM_DATA_UPGRADE"
        }
        response = self.client.execute_workflow('POST', CREATE_DATASOURCE, params=params)
        return response.json()
    
    def delete_edm(self, edm_name: str) -> dict:
        response = self.client.execute_workflow('DELETE', DELETE_DATASOURCE.format(edm_name=edm_name))
        return response.json()
    
    def get_cedants_by_edm(self, edm_name: str) -> dict:
        params = {
            "fields": "id, name",
            "datasource": edm_name,
            "limit": 100
        }
        response = self.client.request('GET', GET_CEDANTS, params=params)
        return response.json()

    def get_lobs_by_edm(self, edm_name: str) -> dict:
        params = {
            "fields": "id, name",
            "datasource": edm_name,
            "limit": 100
        }
        response = self.client.request('GET', GET_LOBS, params=params)
        return response.json()
    