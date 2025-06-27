# Development Guide

In this section we have short guides for getting your development environment up and running.


## Getting familiar with the repo

The `op-analytics` repo has been undergoing a migration from our legacy codebase to a new
python codebase for our data platform. The new codebase is all defined under `src/op-analytics`
so that should be the main entry point to work on new code or find the code for some
functionality in our data pipeline.

### Contents Overview

Here is an overview of some important directories and files in the repo.

- `.circleci`: Configuration for CircleCI jobs.
- `.github`: Configuration for CODEOWNERS.
- `.venv`: Virtual environment managed by `uv`.
- `.vscode`: Shared VS Code settings. Make sure to install all the recommended extensions.
- `dashboards`: Streamlit dashboards.
- `ddl`: CREATE TABLE statements for tables that have been created manually in our databases.
- `demos`: Archive for demo materials.
- `docs`: Directory where HTML for renderd documentation is written out. Shouldn't be edited manually.
- `helm/dagster`: Helm chart configuration for our Dagster deployment. Has information about how Dagster was
   set up and holds the `values.yaml` file that configures all of Dagster (including the user deployments image version).
- `k8s`: Kubernetes manifests. Before moving to Dagster we used to run cronjobs on Kubernetes. We don't do that anymore
   but we still use Kubenetes for lengthy backfill jobs.
- `notebooks`: IPython notebooks. For testing ideas, prototyping pipelines and backfilling.
- `scripts`: Useful shell scripts for adhoc purposes.
- `sphinx`: The source files for the documentation site.
- `src/op_analytics`: Source code for the data platform.
- `tests/op_analytics`: Unit tests for the data platform.
- `Makefile`: Make commands to do things like build docker images or build the documentation site.
- `Dockerfile`: Definition of the op-analytics docker image that gets deployed to Kubernetes.
- `Dockerfile.dagster`: Definition of the op-analytics docker image that gets deployed to Dagster.


The following directories/files are part of our legacy data platform and are still in use:

- `helper_functions`
- `op_chains_tracking`
- `op_collective_economics`
- `op_governance_data`
- `other_chains_tracking`
- `reference_data`
- `rpgf`
- `value_locked_flows`
- `Pipfile`

For the deployment of new pipelines, the general flow of work should be as follows:
- Proof of Concept notebook
- An update secrets with latest credentials
- Data backfill (as is needed/necessary)
- Tests for final functional scripts
- Update of the necssary Dagster and Kubernetes files
- Run  `uv run mypy` to check the new python scripts
- Run the script tests using `uv run pytest tests -vv`
- Create an push the new docker image to helm via `make docker-dagster && make helm-dagster`
- Use `docker images` to confirm that the image has been built

This is a high-level guide of the main steps required to deploy a pipeline. The rest of this guide goes into details on the installation and execution of these steps.

## Virtual Environment

We use [uv](https://docs.astral.sh/uv/) to manage the virtual environment and project dependencies.
After [installing uv](https://docs.astral.sh/uv/getting-started/installation/#standalone-installer)
you can create your development virtualenv by running:
```
$ uv sync
```


## Google Cloud Credentials

Some functionality on this repo requires you to be authenticated with google cloud. You should
install the [gcloud](https://cloud.google.com/sdk/docs/install) cli and then run:
```
gcloud auth application-default login
gcloud config set project oplabs-tools-data
```

Note, if you see the following error then either you are not authenticated or you do not have
access to the required resource:
```
DefaultCredentialsError: Your default credentials were not found. To set up Application Default Credentials
```

## Secrets Management

Whenever we need to make use of a secrete value in our codebase we use the `env_get()` function
part of the `vault.py` module.  This function can load up secrets from multiple locations and
expose them to our code uniformly.

When running on Kubernetes/Dagster secrets are provided via a `SecretProviderClass` kubernetes
resource (see `helm/dagster/secret-provider.yaml` and `k8s/secret-provider.yaml`).  The provider
manifest configures usage to a specific version, which is controlled manually using the Secrets
Manager UI in Google Cloud Platform.

When running locally you can provide secretes using the `.env` file in the repo. This file should
have a single variable called `OP_ANALYTICS_VAULT` which is an encoded string that contains the
secret information. The value for this can be found in the shared team vault.


## Kubernetes + Helm

To work witho our kubernetes cluster and the Dagster helm deployment that runs on it you will
need the `kubectl` and `helm` command-line tools.

- [Instructions to install kubectl](https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl). The main commands to run for the installation are:
   - `gcloud components install kubectl` (actual installation)
   - `kubectl version --client` (conversion of the installation)

- Install helm using brew: `brew install helm`


## Setting up Docker

You will need to [install docker](https://docs.docker.com/get-started/get-docker/) on your laptop.

To be able to publish docker images you will need to create and configure a Github Personal Access
Token (the classic version). Go [here](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#creating-a-personal-access-token-classic) for more instructions.

Please note that if you set expiry date as you generate your token it will need to be updated after expiration and reintroduced to docker login. Also, make sure the following access is added during the token's generation:
- `read:packages` scope to download container images and read their metadata.
- `write:packages` scope to download and upload container images and read and write their metadata.
- `delete:packages` scope to delete container images.

On your personal github settings page (https://github.com/settings/tokens) you need to
"Configure SSO" for the token and grant it access to the `ethereum-optimism` organization.
This will give you permissions  to docker push images on the `ethereum-optimism` container registry.


Once you have your personal access token and you have granted it the right permissions you should
use the `docker login` command to authenticate to the Github Container Registry (ghcr.io).
Follow [this link](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-with-a-personal-access-token-classic) for more instructions on how to pass the token to the `docker login` command.

Here are the package links for the docker images that we publish:

- [`op-analytics-dagster`](https://github.com/orgs/ethereum-optimism/packages/container/op-analytics-dagster)
- [`op-analytics`](https://github.com/orgs/ethereum-optimism/packages/container/package/op-analytics)


## Updating Dagster

To deploy a new version of the code to Dagster you need to first manually bump the Dagster image
version in two places in the codebase:

- `Makefile`
- `helm/dagster/values.yaml`

You can search and replace for the version and replace it in both places at once.

After updatatng the version then run the `make docker-dagster` command to build the image, tag it
with the updated version and push it to the Github Container Registry.

Finally update the Dagster helm chart, telling it about the new image by running `make helm-dagster`.

You can do both of the above at once with `make docker-dagster && make helm-dagster`.

Following this deployment, make sure to update both the dagster and kubernetes secret files after you update the secrets by running:
- `kubectl apply -f helm/dagster/secret-provider.yaml`
- `kubectl apply -f k8s/secret-provider.yaml`

To access the Dagster UI locally, run the following commands (as shown in the output):
- `export DAGSTER_WEBSERVER_POD_NAME=$(kubectl get pods --namespace dagster -l "app.kubernetes.io/name=dagster,app.kubernetes.io/instance=dagster,component=dagster-webserver" -o jsonpath="{.items[0].metadata.name}")`
- `kubectl --namespace dagster port-forward $DAGSTER_WEBSERVER_POD_NAME 8080:80`

Use the following url to view the UI - `http://127.0.0.1:8080`

## Updating the Docs

The source files for the documentation can be found in the `sphinx/` directory. To iterate locally
on the docs before deploying you can open up a terminal and run `make sphinx-serve`.  This will let
you browse the docs over in `localhost:8000`. You can leave the server open and run `make html` on
a different terminal to update the output after editing markdown files.
