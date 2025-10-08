#!/bin/bash

cd /home/suseel/ndap_data_validator

# Activate virtual environment
source /home/suseel/ndap_data_validator/.venv/bin/activate

# Run Uvicorn with Unix socket
# exec uvicorn main:app --uds /home/suseel/ndap_data_validator/ndap_data_validator_app.sock --workers 3
exec uvicorn main:app --host 127.0.0.1 --port 8000 --workers 3
# exec uvicorn main:app --host 0.0.0.0 --port 8005