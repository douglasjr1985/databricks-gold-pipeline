import argparse
import os

from configure_logging import LoggingConfigurator
from databricks_job.DatabricksAPI import DatabricksAPI
from databricks_job.JobConfigGenerator import JobConfigGenerator

def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description='Process modified Jobs.')
    parser.add_argument('--workspace_url', type=str, required=True, help='Workspace URL')
    parser.add_argument('--client_secret', type=str, required=True, help='Client Secret')
    parser.add_argument('--models_dir', type=str, required=True, help='Directory containing notebook models')
    return parser.parse_args()

def main():
    args = parse_arguments()
    databricks_token = args.client_secret
    databricks_instance = args.workspace_url
    models_dir = args.models_dir

    api = DatabricksAPI(databricks_token, databricks_instance)
    
    # Listar arquivos de notebook na pasta models
    notebook_files = [f for f in os.listdir(models_dir) if f.endswith('.ipynb') or f.endswith('.dbc')]
    
    for notebook_file in notebook_files:
        notebook_path = os.path.join(models_dir, notebook_file)
        notebook_name = os.path.splitext(notebook_file)[0]
        
        notebook_task = JobConfigGenerator.generate_notebook_task(notebook_name, notebook_path)
        
        job_config = {
            "name": f"pipeline_{notebook_name}",
            "tasks": [notebook_task],
            "git_source": {
                "git_url": "https://github.com/douglasjr1985/databricks-gold-pipeline.git",
                "git_provider": "gitLab",
                "git_branch": "main"
            },
            "job_clusters": [
                {
                    "job_cluster_key": "automated_cluster_dock",
                    "new_cluster": {
                        "spark_version": "14.3.x-scala2.12",
                         "aws_attributes": {
                                "first_on_demand": 1,
                                "availability": "SPOT_WITH_FALLBACK",
                                "zone_id": "us-east-1a",
                                "instance_profile_arn": "arn:aws:iam::944360682019:instance-profile/DatabricksGlue",
                                "spot_bid_price_percent": 100,
                                "ebs_volume_count": 0
                                },
                        "node_type_id": "c6gd.2xlarge",
                        "num_workers": 2
                    }
                }
            ]
        }
        
        job_id = api.find_job_by_name("pipeline_" + notebook_name)

        if job_id:
            response = api.update_job(job_id, job_config)
        else:
            response = api.create_job(job_config)
        print(f"Job '{notebook_name}' processado com sucesso: {response}")

if __name__ == "__main__":
    main()