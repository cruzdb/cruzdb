#!/bin/bash

set -e
set -x

COVERAGE_ENV=""
if [ "${RUN_COVERAGE}" == 1 ]; then
  COVERAGE_ENV=`bash <(curl -s https://codecov.io/env)`
fi

if [ ! -z ${DOCKER_IMAGE+x} ]; then
  docker run --rm -v ${SOURCE_DIR}:/cruzdb:z,ro \
    -w /cruzdb ${COVERAGE_ENV} -e RUN_COVERAGE \
    ${DOCKER_IMAGE} /bin/bash -c "env && ./ci/before-install.sh && ./ci/install-zlog.sh && ./install-deps.sh && ./ci/run.sh"
else
  ${SOURCE_DIR}/ci/before-install.sh
  ${SOURCE_DIR}/ci/install-zlog.sh
  ${SOURCE_DIR}/install-deps.sh
  ${SOURCE_DIR}/ci/run.sh
fi
