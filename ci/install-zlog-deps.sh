#!/bin/bash

set -e
set -x

BUILD_DIR=$(mktemp -d)
trap "rm -rf ${BUILD_DIR}" EXIT

git clone git://github.com/noahdesu/zlog.git ${BUILD_DIR}
pushd ${BUILD_DIR}
./install-deps.sh
popd
