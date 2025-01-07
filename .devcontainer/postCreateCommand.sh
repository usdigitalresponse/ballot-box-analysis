#! /usr/bin/env bash

# Install OS dependencies
sudo apt-get update \
    && sudo apt-get install -y \
        gdal-bin \
        libgdal-dev \
        g++

# Install dependencies
poetry install --no-root

# Mark the workspace folder as safe
git config --global --add safe.directory ${WORKSPACE}

# Install pre-commit hooks
poetry run pre-commit install --install-hooks
