import requests, time, os
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from .constants import GET_WORKFLOWS

class Client:

    WORKFLOW_COMPLETED = ['FINISHED', 'FAILED', 'CANCELLED'] # https://developer.rms.com/risk-modeler/docs/workflow-engine#polling-workflow-job-and-operation-statuses
    WORKFLOW_IN_PROGRESS = ['QUEUED', 'PENDING', 'RUNNING', 'CANCEL_REQUESTED', 'CANCELLING']

    def __init__(self):
        self.base_url = os.environ.get('RISK_MODELER_BASE_URL', 'https://api-euw1.rms-ppe.com')
        self.api_key = os.environ.get('RISK_MODELER_API_KEY', 'your_api_key')
        self.headers = {'Authorization': self.api_key}
        self.timeout = 200

        session = requests.Session()
        session.headers.update(self.headers or {})

        retry = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "POST", "PUT", "PATCH", "DELETE"),
            raise_on_status=False,
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        session.mount("http://", HTTPAdapter(max_retries=retry))
        self.session = session

    def request(self, method, path, *, full_url=None, base_url=None, params=None, json=None, headers={}, timeout=None, stream=False) -> requests.Response:
        if full_url:
            url = full_url
        else:
            if base_url:
                url = f"{base_url}/{path.lstrip('/')}"
            else:
                url = f"{self.base_url}/{path.lstrip('/')}"

        response = self.session.request(
            method=method,
            url=url,
            params=params,
            json=json,
            headers=self.headers | headers,
            timeout=timeout or self.timeout,
            stream=stream,
        )

        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            # Optional: enrich with server message if available
            msg = ""
            try:
                body = response.json()
                msg = f" | server: {body}"
            except Exception:
                msg = f" | text: {response.text[:500]}"
            raise requests.HTTPError(f"{e} {msg}") from None
        return response
    
    def get_location_header(self, response) -> str:
        if 'location' in response.headers:
            return response.headers['location']
        return ""
    
    def get_workflow_id(self, response) -> str:
        if 'location' in response.headers:
            return response.headers['location'].split('/')[-1]
        return ""

    def poll_workflow(self, workflow_url, interval=10, timeout=600000) -> requests.Response:
        if not workflow_url:
            raise ValueError(f"Invalid workflow URL provided for polling: {workflow_url}.")
        
        start = time.time()
        while True:
            print(f"Polling workflow url {workflow_url}")
            response = self.request('GET', '', full_url=workflow_url)
            print(f"Workflow status: {response.json().get('status', '')}; Percent complete: {response.json().get('progress', '')}")

            status = response.json().get('status', '')
            if status in self.WORKFLOW_COMPLETED:
                return response
            
            if time.time() - start > timeout:
                raise TimeoutError(f"Workflow did not complete within {timeout} seconds.")
            time.sleep(interval)

    def poll_workflow_batch(self, workflow_ids, interval=20, timeout=600000) -> requests.Response:
        start = time.time()
        while True:
            print(f"Polling batch workflow ids: {','.join(str(item) for item in workflow_ids)}")
            params = {'ids': ','.join(str(item) for item in workflow_ids)}
            response = self.request('GET', GET_WORKFLOWS, params=params)

            all_completed = True
            for workflow in response.json().get('workflows', []):
                status = workflow.get('status', '')
                if status in self.WORKFLOW_IN_PROGRESS:
                    all_completed = False
                    break

            if all_completed:
                return response
            
            if time.time() - start > timeout:
                raise TimeoutError(f"Batch workflows did not complete within {timeout} seconds.")
            time.sleep(interval)

    def execute_workflow(self, method, path, *, params=None, json=None, headers={}, timeout=None, stream=False) -> requests.Response:
        print("Submitting workflow request...")
        response = self.request(method, path, params=params, json=json, headers=headers, timeout=timeout, stream=stream)
        if response.status_code not in (201,202):
            return response
        else:
            workflow_url = self.get_location_header(response)
            return self.poll_workflow(workflow_url)