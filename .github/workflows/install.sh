#!/bin/bash

chmod -R 777 /home/suseel/ndap_data_validator

echo "Deploying .. . ..."
# UV install using wget
wget -qO- https://astral.sh/uv/install.sh | sh
# To add $HOME/.local/bin to your PATH
. $HOME/.local/bin/env

# uv sync to sync dependencies in .venv
uv sync


# add www-data to the group that owns the socket for nginx:
# sudo usermod -aG splinkapiadm www-data
cd /home/suseel/ndap_data_validator

uv run uvicorn main:app