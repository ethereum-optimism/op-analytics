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


# Exctract all lines between the /START/,/END/ patterns (inclusive).
# These lines contain the dependencies list.
awk '/^dependencies =/,/^]/' pyproject.toml  >> dummydeps/pyproject.toml
