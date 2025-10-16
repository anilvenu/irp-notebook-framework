from .client import Client

class PortfolioManager:
    def __init__(self, client: Client):
        self.client = client

    def create_portfolio(self, edm_name: str, portfolio_name: str, portfolio_number: str = "1", description: str = "") -> dict:
        params = {"datasource": edm_name}
        data = {
            "name": portfolio_name,
            "number": portfolio_number,
            "description": description,
        }
        response = self.client.request('POST', '/riskmodeler/v2/portfolios', params=params, json=data)
        return {'id': response.headers['location'].split('/')[-1]}

    def get_portfolios_by_edm_name(self, edm_name: str) -> dict:
        params = {"datasource": edm_name}
        response = self.client.request('GET', '/riskmodeler/v2/portfolios', params=params)
        return response.json()
    
    def get_portfolio_by_edm_name_and_id(self, edm_name: str, portfolio_id: int) -> dict:
        params = {"datasource": edm_name}
        response = self.client.request('GET', f'/riskmodeler/v2/portfolios/{portfolio_id}', params=params)
        return response.json()
    
    # TODO: Refactor repeating code blocks
    def geohaz_portfolio(self,
                         edm_name: str,
                         portfolio_id: int,
                         *,
                         geocode: bool = True,
                         hazard_eq: bool = False,
                         hazard_ws: bool = False,
                         engine_type: str = "RL",
                         version: str = "22.0",
                         geocode_layer_options: dict = {"aggregateTriggerEnabled": "true", "geoLicenseType": "0", "skipPrevGeocoded": False},
                         hazard_layer_options: dict = {"overrideUserDef": False,"skipPrevHazard": False}
                        ) -> dict:
        params = {"datasource": edm_name}
        data = []
        if geocode:
            data.append(
                {
                    "name": "geocode",
                    "type": "geocode",
                    "engineType": engine_type,
                    "version": version,
                    "layerOptions": geocode_layer_options,
                }
            )
        if hazard_eq:
            data.append(
                {
                    "name": "earthquake",
                    "type": "hazard",
                    "engineType": engine_type,
                    "version": version,
                    "layerOptions": hazard_layer_options
                }
            )
        if hazard_ws:
            data.append(
                {
                    "name": "windstorm",
                    "type": "hazard",
                    "engineType": engine_type,
                    "version": version,
                    "layerOptions": hazard_layer_options
                }
            )

        response = self.client.execute_workflow('POST',
                                                f"/riskmodeler/v2/portfolios/{portfolio_id}/geohaz",
                                                params=params,
                                                json=data)
        return response.json()