#!/bin/bash

set -e
set -x

THIS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR=${THIS_DIR}/../

COVERAGE_ENV=""
if [ "${RUN_COVERAGE}" == 1 ]; then
  COVERAGE_ENV=`bash <(curl -s https://codecov.io/env)`
fi

if [ ! -z ${DOCKER_IMAGE+x} ]; then

  # maybe build and use image with all dependencies
  if [ -n "${NO_DEP_IMG}" ]; then
    depimg=${DOCKER_IMAGE}
    pre_run="./ci/before-install.sh && ./ci/install-zlog.sh && ./install-deps.sh &&"
  else
    depid=$(md5sum ${ROOT_DIR}/install-deps.sh | cut -c -32)
    depimg=smf_test_deps_${DOCKER_IMAGE//:/_}_${depid}
    if [[ "$(docker images -q ${depimg} 2> /dev/null)" == "" ]]; then
      # remove the cidfile file to make docker run happy
      cidfile=$(mktemp); rm ${cidfile}
      docker run -it --cidfile=${cidfile} \
        -e USE_CLANG=1 -v ${ROOT_DIR}:/cruzdb:z,ro -w /cruzdb ${DOCKER_IMAGE} \
        /bin/bash -c "./ci/before-install.sh && ./ci/install-zlog-deps.sh && ./install-deps.sh"
      cid=$(cat ${cidfile})
      docker commit ${cid} ${depimg}
      rm ${cidfile}
    fi
    pre_run=
  fi

  # TODO: add another layer here for zlog, so we can easily refresh just those
  # depenencies when they change. perhaps we could also query out to github for
  # the sha1 and only update if its changed

  docker run --rm -v ${ROOT_DIR}:/cruzdb:z,ro \
    -t -w /cruzdb ${COVERAGE_ENV} -e RUN_COVERAGE \
    ${depimg} /bin/bash -c "env && ${pre_run} ./ci/install-zlog.sh && ./ci/run.sh"

else
  ${ROOT_DIR}/ci/before-install.sh
  ${ROOT_DIR}/ci/install-zlog-deps.sh
  ${ROOT_DIR}/ci/install-zlog.sh
  ${ROOT_DIR}/install-deps.sh
  ${ROOT_DIR}/ci/run.sh
fi
