import requests
import json

class DatabricksAPI:
    def __init__(self, token, instance):
        self.token = token
        self.instance = instance
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def create_job(self, job_config):
        url = f"{self.instance}/api/2.0/jobs/create"
        response = requests.post(url, headers=self.headers, json=job_config)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

    def update_job(self, job_id, new_job_config):
        url = f"{self.instance}/api/2.0/jobs/update"
        update_config = {"job_id": job_id, "new_settings": new_job_config}
        response = requests.post(url, headers=self.headers, json=update_config)
        response.raise_for_status()
        return response.json()

    def find_job_by_name(self, job_name):
        url = f"{self.instance}/api/2.0/jobs/list"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()

        jobs = response.json().get('jobs', [])
        for job in jobs:
            if job['settings']['name'] == job_name:
                return job['job_id']
        return None

    def create_or_update_job(self, job_config):
        job_id = self.find_job_by_name(job_config['name'])
        if job_id:
            return self.update_job(job_id, job_config)
        else:
            return self.create_job(job_config)
