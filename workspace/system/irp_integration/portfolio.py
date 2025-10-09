from .client import Client

class PortfolioManager:
    def __init__(self, client: Client):
        self.client = client

    def create_portfolio(self, edm_name: str, portfolio_name: str, portfolio_number: str = "1", description: str = "") -> dict:
        params = {"datasource": edm_name}
        data = {
            "name": portfolio_name,
            "number": portfolio_number,
            "description": "This is a test portfolio created from a notebook",
        }
        response = self.client.request('POST', '/riskmodeler/v2/portfolios', params=params, json=data)
        return response.json()

    def get_portfolios_by_edm_name(self, edm_name: str) -> dict:
        params = {"datasource": edm_name}
        response = self.client.request('GET', '/riskmodeler/v2/portfolios', params=params)
        return response.json()
    
    def get_portfolio_by_edm_name_and_id(self, edm_name: str, portfolio_id: int) -> dict:
        params = {"datasource": edm_name}
        response = self.client.request('GET', f'/riskmodeler/v2/portfolios/{portfolio_id}', params=params)
        return response.json()
    
    def geocode_portfolio(self,
                          edm_name: str,
                          portfolio_id: int,
                          *,
                          name: str = "geocode",
                          type: str = "geocode",
                          engine_type: str = "RL",
                          version: str = "21.0",
                          layer_options: dict = {"aggregateTriggerEnabled": "true", "geoLicenseType": "0", "skipPrevGeocoded": False}
                        ) -> dict:
        params = {"datasource": edm_name}
        data = [
            {
                "name": name,
                "type": type,
                "engineType": engine_type,
                "version": version,
                "layerOptions": layer_options,
            }
        ]

        response = self.client.execute_workflow('POST',
                                                f"/riskmodeler/v2/portfolios/{portfolio_id}/geohaz",
                                                params=params,
                                                json=data)
        return response.json()