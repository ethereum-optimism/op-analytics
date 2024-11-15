# syntax=docker/dockerfile:1.9
FROM python:3.12-slim-bookworm


# Install tiny (add more system packages here if needed)
RUN <<EOT
apt-get update -qy
apt-get install -qyy \
    -o APT::Install-Recommends=false \
    -o APT::Install-Suggests=false \
    tini

apt-get clean
rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
EOT


# Install the package
COPY dist/ /_dist/
RUN --mount=type=cache,target=/root/.cache <<EOT
cd /_dist
pip install op_analytics-24.11.140-py3-none-any.whl
EOT


# Don't run as root.
RUN <<EOT
groupadd -r app
useradd -r -d /app -g app -N app
EOT


# This is a dummy entrypoing. Override it in kubernetes manifest.
ENTRYPOINT ["tini", "-v", "--", "opdata", "chains", "health"]
# See <https://hynek.me/articles/docker-signals/>.
STOPSIGNAL SIGINT
