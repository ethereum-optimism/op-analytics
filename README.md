# OP Analytics
Onchain Data, Utilities, References, and other Analytics on Optimism. Join the conversation with other numba nerds in the #analytics channel in the Optimism Discord.

## Key Links
#### 📄 [OP Analytics Repo Documentation](https://static.optimism.io/op-analytics/sphinx/html/index.html)

#### ⚕️ [Optimism Superchain Health Dashboard](https://docs.google.com/spreadsheets/d/1f-uIW_PzlGQ_XFAmsf9FYiUf0N9l_nePwDVrw0D5MXY/edit?gid=584971628#gid=584971628)
  ![https://docs.google.com/spreadsheets/d/1f-uIW_PzlGQ_XFAmsf9FYiUf0N9l_nePwDVrw0D5MXY/edit?gid=584971628#gid=584971628](https://github.com/user-attachments/assets/692759b0-32ec-43db-8bea-e22aa8611d92)
#### 🧮 [Superchain Top Contracts by Transactions to](https://app.hex.tech/61bffa12-d60b-484c-80b9-14265e268538/app/cd3f1525-08f0-4a49-a15a-b72f46f2a0d8/latest)

---

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
