# Setting up for deploying code

## Kubernetes + Helm

Installing kubectl
https://cloud.google.com/kubernetes-engine/docs/how-to/cluster-access-for-kubectl

Installing helm
```
$ brew install helm
```

## Publishing Docker images

Creating a personal access token:
https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#creating-a-personal-access-token-classic

On your https://github.com/settings/tokens you need to "Configure SSO" for the token
and grant it access to the `ethereum-optimism` organization. This will give you permissions 
to docker push images on the `ethereum-optimism` container registry.


Docker login with our personal access token:
https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry#authenticating-with-a-personal-access-token-classic


## `op-analytics-dagster` docker image

https://github.com/orgs/ethereum-optimism/packages/container/op-analytics-dagster/settings