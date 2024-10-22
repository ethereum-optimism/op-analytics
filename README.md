# OP Analytics
Onchain Data, Utilities, References, and other Analytics on Optimism. Join the conversation with other numba nerds in the #analytics channel in the Optimism Discord.


### Installation
```
python -m pip install pipenv
pipenv install
```
See `Pipfile` for all the requirements.

### Common Requirements
Common packages used for python scripts include
- [pandas](https://github.com/pandas-dev/pandas)
- [requests](https://github.com/psf/requests)
- [aiohttp-retry](https://github.com/inyutin/aiohttp_retry)
- [dune-client](https://github.com/cowprotocol/dune-client)
- [subgrounds](https://github.com/0xPlaygrounds/subgrounds)
- [web3.py](https://github.com/ethereum/web3.py)
- [ethereum-etl](https://github.com/blockchain-etl/ethereum-etl)

In this repository, we use `pre-commit` to ensure consistency of formatting. To install for Mac, run
```
brew install pre-commit
```
Once installed, in the command line of the repository, run
```
pre-commit install
```
This will install `pre-commit` to the Git hook, so that `pre-commit` will run and fix files covered in its config before committing.
