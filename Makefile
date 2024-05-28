# Define variables
PYTHON := python3
PIP := pip
PYTEST := pytest

# Install dependencies
install:
	$(PIP) install -r requirements.txt

# Run the Python script with modified files and deploy
deploy: 
	$(PYTHON) main.py --workspace_url "$(DATALAKE_DATABRICKS_WORKSPACE_URL_PRD)" --client_secret "$(DATALAKE_DATABRICKS_CLIENT_SECRET)" --filename "$$filename"; 

# Default target
all: install deploy