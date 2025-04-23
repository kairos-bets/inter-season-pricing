# Inter-season pricing

This repository contains exploratory code to price post-transfer player goal scoring probabilities (e.g. how do you model the probability of Mbappé scoring in his first match after being transferred from PSG to Real Madrid?).

## Installation

### Miniconda install

(Only if you don't already have a working install)

```
mkdir -p ~/miniconda3
curl https://repo.anaconda.com/miniconda/Miniconda3-latest-MacOSX-arm64.sh -o ~/miniconda3/miniconda.sh
bash ~/miniconda3/miniconda.sh -b -u -p ~/miniconda3
rm ~/miniconda3/miniconda.sh
```

### Setup the conda env

```
source ~/miniconda3/bin/activate
conda create -n pricing python=3.10
conda activate pricing
pip install poetry
poetry install
pre-commit install
```

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
  - We will only use transfer data from transfermarkt

- From FBref

  - We use player match log data from FBref for all the analysis here

- Focus on transfers to top 5 EU leagues

Our process:

- use transfer information from transfermarkt to select relevant transfers (player transferred to a club in the top 5 EU leagues in the past 5 seasons)
- create a dataset with the first X games for these players right after they have been transferred (the test dataset)
- create a train dataset with all the other games for all players in these top 5 EU leagues (i.e. non post-transfer games). We have to be careful here because we need to keep post-transfer games for the feature engineering that will be used in our training set, we just don't want to train the model on the post-transfer lines
- enrich the test dataset with information about the games these players played in the championship right before they transferred to be able to do our feature engineering
- perform feature engineering on train and enriched test

## Features

## Modeling

- Logistic regression with good feature engineering
- Hierarchical Bayesian?
