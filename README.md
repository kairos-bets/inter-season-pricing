# Inter-season pricing

This repository contains exploratory code to price post-transfer player goal scoring probabilities (e.g. how do you model the probability of Mbappé scoring in his first match after being transferred from PSG to Real Madrid?).

## Installation

### Miniconda install

mkdir -p ~/miniconda3
curl https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-arm64.sh -o ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm ~/miniconda3/miniconda.sh

### Setup the conda env

source ~/miniconda3/bin/activate
conda create -n pricing python=3.10
conda activate pricing
pip install poetry
poetry install
pre-commit install

## Development

### Conventions & guidelines

- We use `poetry` to manage dependencies.
- We use `pre-commit` to manage pre-commit hooks.
- We use `black` to format the code.
- We use `isort` to sort the imports.
- We use `mypy` to type check the code.
- We use `pytest` to run the tests.

## Data

- From transfermarkt

  - Careful: tranfermarkt lineup data stops midway in 2024, and there are also missing lineups (e.g. for Alexis Sanchez there are missing matches for 2023 from september to december)

- From FBref
- Focus on transfers to top 5 EU leagues

## Features

## Modeling

- Logistic regression with good feature engineering
- Hierarchical Bayesian?
