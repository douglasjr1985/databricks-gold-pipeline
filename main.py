import argparse
import os
import json
import requests
from loguru import logger
from configure_logging import LoggingConfigurator
from databricks_job.DatabricksAPI import DatabricksAPI
from databricks_job.JobConfigGenerator import JobConfigGenerator


def parse_arguments():
    """Parses command-line arguments."""
    parser = argparse.ArgumentParser(description='Process modified Jobs.')
    parser.add_argument('--workspace_url', type=str, required=True, help='Workspace URL')
    parser.add_argument('--client_secret', type=str, required=True, help='Client Secret')
    parser.add_argument('--projects_dir', type=str, required=True, help='Directory containing project folders')
    return parser.parse_args()


def list_tasks(directory):
    """Lists task directories in the specified directory."""
    task_directories = []
    for root, dirs, _ in os.walk(directory):
        for dir in dirs:
            task_directories.append(os.path.join(root, dir))
    return task_directories


def load_job_config(project_path):
    """Loads job configuration from the project directory."""
    config_path = os.path.join(project_path, 'jobconfig.json')
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            return json.load(f)
    else:
        raise FileNotFoundError(f"Configuration file not found in project: {project_path}")


def load_query(task_path):
    """Loads SQL query from the task directory."""
    query_path = os.path.join(task_path, 'query.sql')
    if os.path.exists(query_path):
        with open(query_path, 'r') as f:
            return f.read()
    else:
        raise FileNotFoundError(f"Query file not found in task: {task_path}")


def load_config(task_path):
    """Loads table config from the task directory."""
    config_path = os.path.join(task_path, 'config_table.json')
    if os.path.exists(config_path):
        with open(config_path, 'r') as f:
            return json.load(f)
    else:
        raise FileNotFoundError(f"Config file not found in task: {task_path}")


def main():
    args = parse_arguments()
    databricks_token = args.client_secret
    databricks_instance = args.workspace_url
    projects_dir = args.projects_dir

    api = DatabricksAPI(databricks_token, databricks_instance)
    executor_script_path = 'dbfs:/path/to/executor.py'  # Path to the executor script in DBFS

    # Iterate over each project in the projects directory
    for project_name in os.listdir(projects_dir):
        project_path = os.path.join(projects_dir, project_name)
        if os.path.isdir(project_path) and project_name != 'models':
            tasks = []
            logger.info(f"Processing project: {project_name}")
            for task_name in os.listdir(project_path):
                task_path = os.path.join(project_path, task_name)
                if os.path.isdir(task_path):
                    logger.info(f"Processing task: {task_name} in project: {project_name}")
                    # Load SQL query from the task directory
                    try:
                        sql_query = load_query(task_path)
                        logger.info(f"Loaded query for task {task_name}: {sql_query}")
                    except FileNotFoundError as e:
                        logger.error(e)
                        continue

                    # Load table config
                    try:
                        config = load_config(task_path)
                        logger.info(f"Loaded config for task {task_name}: {config}")
                    except FileNotFoundError as e:
                        logger.error(e)
                        continue

                    # Load job config to get the job_cluster_key
                    try:
                        job_cluster_config = load_job_config(project_path)
                        job_cluster_key = job_cluster_config["job_cluster_key"]
                    except FileNotFoundError as e:
                        logger.error(e)
                        continue

                    # Generate a task to execute the SQL query using the Python script
                    python_task = JobConfigGenerator.generate_python_task(
                        task_name,
                        base_parameters={
                            "sql_query": sql_query,
                            "database": config["database"],
                            "table_name": config["table_name"],
                            "mode": config.get("mode", "append")  # Default para append se não especificado
                        }
                    )
                    tasks.append(python_task)
                    logger.info(f"Task {task_name} added to project {project_name}")

            if not tasks:
                logger.warning(f"No tasks found for project {project_name}")

            job_config = {
                "name": project_name,
                "tasks": tasks,
                "git_source": {
                    "git_url": "https://github.com/douglasjr1985/databricks-gold-pipeline.git",
                    "git_provider": "gitHub",
                    "git_branch": "main"
                },
                "job_clusters": [
                    {
                        "job_cluster_key": job_cluster_config["job_cluster_key"],
                        "new_cluster": job_cluster_config["new_cluster"]
                    }
                ]
            }

            # Log to verify job_config
            logger.info(f"Job config for project {project_name}: {json.dumps(job_config, indent=4)}")

            try:
                job_id = api.find_job_by_name(project_name)
                if job_id:
                    response = api.update_job(job_id, job_config)
                else:
                    response = api.create_job(job_config)
                logger.info(f"Job processed successfully for project {project_name}: {response}")
            except requests.exceptions.HTTPError as e:
                logger.error(f"Failed to process job for project {project_name}: {e.response.text}")


if __name__ == "__main__":
    main()
