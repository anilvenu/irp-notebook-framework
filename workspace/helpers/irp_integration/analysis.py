import json
from .client import Client

class AnalysisManager:
    def __init__(self, client: Client, reference_data_manager=None):
        self.client = client
        self._reference_data_manager = reference_data_manager

    @property
    def reference_data_manager(self):
        if self._reference_data_manager is None:
            # Lazy import to avoid circular dependencies
            from .reference_data import ReferenceDataManager
            self._reference_data_manager = ReferenceDataManager(self.client)
        return self._reference_data_manager

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

    def submit_analysis_job(self, 
                            job_name: str, 
                            edm_name: str, 
                            portfolio_id: int, 
                            analysis_profile_name: str, 
                            output_profile_name: str, 
                            event_rate_scheme_name: str, 
                            treaty_ids: list,
                            tag_names: list,
                            *,
                            global_analysis_settings: dict = {"franchiseDeductible": False,"minLossThreshold": "1.00","treatConstructionOccupancyAsUnknown": True,"numMaxLossEvent": 1},
                            currency: dict = {} # TODO
                        ) -> int:
        model_profile_response = self.get_model_profiles_by_name([analysis_profile_name])
        output_profile_response = self.get_output_profile_by_name(output_profile_name)
        event_rate_scheme_response = self.get_event_rate_scheme_by_name(event_rate_scheme_name)
        tag_ids = self.reference_data_manager.get_tag_ids_from_tag_names(tag_names)

        if model_profile_response['count'] > 0 and len(output_profile_response) > 0 and event_rate_scheme_response['count'] > 0:
            data = {
                "currency": { # TODO
                    "asOfDate": "2018-11-15",
                    "code": "USD",
                    "scheme": "RMS",
                    "vintage": "RL18.1"
                },
                "edm": edm_name,
                "eventRateSchemeId": event_rate_scheme_response['items'][0]['eventRateSchemeId'],
                "exposureType": "PORTFOLIO",
                "id": portfolio_id,
                "modelProfileId": model_profile_response['items'][0]['id'],
                "outputProfileId": output_profile_response[0]['id'],
                "treaties": treaty_ids,
                "tagIds": tag_ids,
                "globalAnalysisSettings": global_analysis_settings,
                "jobName": job_name
            }

            print(json.dumps(data, indent=2))

            response = self.client.request('POST', f"/riskmodeler/v2/portfolios/{portfolio_id}/process", json=data)
            return int(response.headers['location'].split('/')[-1])
        return -1
    
    def poll_analysis_job_batch(self, workflow_ids: list) -> dict:
        response = self.client.poll_workflow_batch(workflow_ids)
        return response.json()

    def analyze_portfolio(self,
                          job_name: str,
                          edm_name: str,
                          portfolio_id: int,
                          model_profile_id: int,
                          output_profile_id: int,
                          event_rate_scheme_id: int,
                          treaty_ids: list,
                          *,
                          global_analysis_settings: dict = {"franchiseDeductible": False,"minLossThreshold": "1.00","treatConstructionOccupancyAsUnknown": True,"numMaxLossEvent": 1},
                          currency: dict = {} # TODO
                        ) -> dict:
        data = {
            "currency": { # TODO
                "asOfDate": "2018-11-15",
                "code": "USD",
                "scheme": "RMS",
                "vintage": "RL18.1"
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
            "globalAnalysisSettings": global_analysis_settings,
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
    
    def create_analysis_group(self, 
                              analysis_ids: list, 
                              group_name: str, 
                              *,
                              simulate_to_plt: bool = True,
                              num_simulations: int = 50000,
                              propogate_detailed_losses: bool = False,
                              reporting_window_start: str = "01/01/2021",
                              simulation_window_start: str = "01/01/2021",
                              simulation_window_end: str = "12/31/2021",
                              region_peril_simulation_set: list = [],
                              description: str = ""
                              ) -> dict:
        data = {
            "analysisIds": analysis_ids,
            "name": group_name,
            "currency": { # TODO
                "asOfDate": "2018-11-15",
                "code": "USD",
                "scheme": "RMS",
                "vintage": "RL18.1"
            },
            "simulateToPLT": simulate_to_plt,
            "numOfSimulations": num_simulations,
            "propagateDetailedLosses": propogate_detailed_losses,
            "reportingWindowStart": reporting_window_start,
            "simulationWindowStart": simulation_window_start,
            "simulationWindowEnd": simulation_window_end,
            "regionPerilSimulationSet": region_peril_simulation_set,
            "description": description
        }

        print(json.dumps(data, indent=2))

        response = self.client.execute_workflow('POST', '/riskmodeler/v2/analysis-groups', json=data)
        return response.json()
        
