import json
import os
from .client import Client
from .constants import EXPORT_TO_RDM

class RDMManager:
    def __init__(self, client: Client, analysis_manager=None):
        self.client = client
        self._analysis_manager = analysis_manager

    @property
    def analysis_manager(self):
        if self._analysis_manager is None:
            # Lazy import to avoid circular dependencies
            from .analysis import AnalysisManager
            self._analysis_manager = AnalysisManager(self.client)
        return self._analysis_manager

    def export_analyses_to_rdm(self, rdm_name: str, analysis_ids: list) -> dict:
        analyses = self.analysis_manager.get_analyses_by_ids(analysis_ids)
        resourceUris = []
        for analysis in analyses:
            resourceUris.append(analysis['uri'])

        data = {
            "exportType": "RDM_DATABRIDGE",
            "resourceType": "analyses",
            "resourceUris": resourceUris,
            "settings": {
                "serverId": 88094, # TODO
                "rdmName": rdm_name
            }
        }

        print(json.dumps(data, indent=2))

        response = self.client.execute_workflow('POST', EXPORT_TO_RDM, json=data)
        return response.json()