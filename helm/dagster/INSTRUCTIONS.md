
# Installing the dagster helm chart

Here is the procedure that was used to install the dagster helm chart.
```
$ helm repo add dagster https://dagster-io.github.io/helm
$ helm show values dagster/dagster > helm/dagster/values.yaml
$ kubectl create namespace dagster
$ helm upgrade --install dagster dagster/dagster -f helm/dagster/values.yaml -n dagster
```

To expose the dagster webserver we created a self-signed certificate and used to configure
a traefik ingressroute.
```
$ openssl genpkey -algorithm RSA -out cert.key -pkeyopt rsa_keygen_bits:2048
$ openssl req -new -key cert.key -out cert.csr
$ openssl x509 -req -in cert.csr -signkey cert.key -out cert.crt -days 365
$ kubectl create secret tls dagster-cert-tls --cert=cert.crt --key=cert.key -n dagster
$ kubectl apply -f helm/dagster/ingressroute.yaml -n dagster
```

To provide access to secrets from job kubernetes pods we created a service account 
and secret provider.
```
$ kubectl apply -f helm/dagster/secret-provider.yaml
$ kubectl apply -f helm/dagster/service-account.yaml
```

Refer to the Google Cloud documentation for more information on how to grant secrets access to
a kubernetes service account.

# Updating the dagster helm chart

When the dagster definitions docker image changes we need to update the image tag in values.yaml
and re-deploy the helm chart
```
$ helm upgrade dagster dagster/dagster -f helm/dagster/values.yaml -n dagster
```