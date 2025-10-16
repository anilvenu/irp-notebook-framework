from .client import Client

class AnalysisManager:
    def __init__(self, client: Client):
        self.client = client

    def get_model_profiles_by_name(self, profile_names: list) -> dict:
        params = {'name': profile_names}
        response = self.client.request('GET', '/analysis-settings/modelprofiles', params=params)
        return response.json()

    def get_analysis_by_id(self, analysis_id: int) -> dict:
        params = {"q": f"id={analysis_id}"}
        response = self.client.request('GET', '/riskmodeler/v2/analyses', params=params)
        return response.json()
    
    def get_output_profile_by_name(self, profile_name: str) -> dict:
        params = {'name': profile_name}
        response = self.client.request('GET', '/analysis-settings/outputprofiles', params=params)
        return response.json()
    
    def get_event_rate_scheme_by_name(self, scheme_name: str) -> dict:
        params = {'where': f"eventRateSchemeName=\"{scheme_name}\""}
        response = self.client.request('GET', '/data-store/referencetables/eventratescheme', params=params)
        return response.json()

    def analyze_portfolio(self,
                          job_name: str,
                          edm_name: str,
                          portfolio_id: int,
                          model_profile_id: int,
                          output_profile_id: int,
                          event_rate_scheme_id: int,
                          treaty_ids: list
                        ) -> dict:
        data = {
            "currency": { # TODO
                "asOfDate": "2023-05-12",
                "code": "EUR",
                "scheme": "Test new scheme_185",
                "vintage": "test2"
            },
            "edm": edm_name,
            "eventRateSchemeId": event_rate_scheme_id,
            "exposureType": "PORTFOLIO",
            "id": portfolio_id,
            "modelProfileId": model_profile_id,
            "outputProfileId": output_profile_id,
            "treaties": treaty_ids,
            # "tagIds": [ # TODO
            #     1202
            # ],
            "globalAnalysisSettings": {
                "franchiseDeductible": False,
                "minLossThreshold": "1.00",
                "treatConstructionOccupancyAsUnknown": True,
                "numMaxLossEvent": 1
            },
            "jobName": job_name
        }

        response = self.client.execute_workflow('POST', f"/riskmodeler/v2/portfolios/{portfolio_id}/process", json=data)
        return response.json()
    
    def execute_analysis(self, job_name: str, edm_name: str, portfolio_id: int, analysis_profile_name: str, output_profile_name: str, event_rate_scheme_name: str, treaty_ids: list) -> dict:
        model_profile_response = self.get_model_profiles_by_name([analysis_profile_name])
        output_profile_response = self.get_output_profile_by_name(output_profile_name)
        event_rate_scheme_response = self.get_event_rate_scheme_by_name(event_rate_scheme_name)
        if model_profile_response['count'] > 0 and len(output_profile_response) > 0 and event_rate_scheme_response['count'] > 0:
            return self.analyze_portfolio(job_name, 
                                          edm_name, 
                                          portfolio_id, 
                                          model_profile_response['items'][0]['id'], 
                                          output_profile_response[0]['id'], 
                                          event_rate_scheme_response['items'][0]['eventRateSchemeId'],
                                          treaty_ids)
        return {}
    
    # def create_analysis_group(self, analysis_ids: list, group_name: str, simulate_to_plt: bool = True, )
        
