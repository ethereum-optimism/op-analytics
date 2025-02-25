#!/bin/bash

################################################################################
#  Create a dummy pyproject.toml which only has the dependencies of the main
#  project.
#  
#  This file is used when building a docker image, so we can have a cached
#  layer that only includes the project's dependencies.
#
#  The dependencies don't change as frequently as the application itself, so
#  when we modify the application we can get get much faster docker builds and
#  also save on uploads because the layer with all the dependencies is already
#  cached.
################################################################################

mkdir -p dummydeps

cat << EOF > dummydeps/pyproject.toml
[project]
name = "op-anlytics-deps"
version = "0.0.1"
description = "op-analytics dependencies."
requires-python = ">=3.12"
EOF


# Extract all lines between the /START/,/END/ patterns (inclusive), where
#
#   START = '^dependencies'
#     END = '^]'
#
# These lines contain the dependencies in pyproject.toml.
awk '/^dependencies =/,/^]/' pyproject.toml  | grep -v "sugar" >> dummydeps/pyproject.toml

# NOTE: In the above command we remove the "sugar" dependency. The sugar-sdk is not
# published to PyPI so it is installed manually in the Dockerfile.
