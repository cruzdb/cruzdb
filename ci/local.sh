#!/bin/bash

set -e
set -x

THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR=${THIS_DIR}/../

DOCKER_IMAGES="
ubuntu:xenial
fedora:26
"

for img in ${DOCKER_IMAGES}; do
  DOCKER_IMAGE=${img} RUN_COVERAGE=0 ci/script.sh
done
