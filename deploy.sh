#!/bin/bash

cd /home/ziplime
# install dependencies
pip3 install poetry
poetry config virtualenvs.create false
poetry build
poetry config pypi-token.pypi $PYPI_TOKEN
poetry publish



