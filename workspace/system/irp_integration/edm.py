from .client import Client

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
        response = self.client.request('GET', '/riskmodeler/v2/datasources')
        return response.json()
    
    def get_edm_by_name(self, edm_name: str) -> dict:
        params = {"q": f"datasourceName={edm_name}"}
        response = self.client.request('GET', '/riskmodeler/v2/datasources', params=params)
        return response.json()

    def create_edm(self, edm_name: str, server_name: str) -> dict:
        params = {
            "datasourcename": edm_name,
            "servername": server_name,
            "operation": "CREATE"
        }
        response = self.client.execute_workflow('POST', '/riskmodeler/v2/datasources', params=params)
        return response.json()
    
    def duplicate_edm(self, source_edm_name: str, dest_edm_name: str = "") -> dict:
        if not dest_edm_name:
            dest_edm_name = f"np_{source_edm_name}"
        
        portfolios_response = self.portfolio_manager.get_portfolios_by_edm_name(source_edm_name)
        portfolio_ids = []
        for portfolio in portfolios_response['searchItems']:
            portfolio_ids.append(portfolio['id'])

        data = data = {
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

        response = self.client.execute_workflow('POST', '/riskmodeler/v2/exports', json=data)
        return response.json()
    
    def delete_edm(self, edm_name: str) -> dict:
        response = self.client.execute_workflow('DELETE', f'/riskmodeler/v2/datasources/{edm_name}')
        return response.json()