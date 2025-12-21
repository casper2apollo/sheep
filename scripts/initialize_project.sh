#!/bin/bash

python3.12 -m venv --prompt "SHEEP" .venv
source .venv/bin/activate
pip install --upgrade pip setuptools wheel
pip install -e .[dev]