import json

class JobConfigGenerator:
    @staticmethod
    def generate_job_config_from_json(json_file_path):
        with open(json_file_path, 'r') as json_file:
            projects_json = json.load(json_file)
        return [project for project in projects_json if project['active']]


    @staticmethod
    def generate_notebook_task(notebook_name, notebook_path):
        return {
            "task_key": notebook_name,
            "description": f"Task for {notebook_name}",
            "job_cluster_key": "automated_cluster_dock",
            "depends_on": [{"task_key": "bootstrap_task"}],
            "libraries": [],
            "notebook_task": {
                "notebook_path": notebook_path
            }
        }  

    @staticmethod
    def generate_bootstrap_task():
        return {
            "task_key": "bootstrap_task",
            "description": "Cluster initialization task",
            "job_cluster_key": "automated_cluster_dock",
            "libraries": [
                            {
                                "pypi": {
                                    "package": "ydata-profiling==4.0.0"  
                                }
                            }
                        ],       
            "spark_python_task": {
                "python_file": "scripts/bootstrap_task.py",
                "parameters": [ ],
                "source": "GIT"
            }
            }




