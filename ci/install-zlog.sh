#!/bin/bash

set -e
set -x

if test $(id -u) != 0 ; then
  SUDO=sudo
fi

CMAKE_INSTALL_PREFIX=/usr
if [[ "$OSTYPE" == "darwin"* ]]; then
  CMAKE_INSTALL_PREFIX=/usr/local
fi

BUILD_DIR=$(mktemp -d)
trap "rm -rf ${BUILD_DIR}" EXIT

git clone git://github.com/noahdesu/zlog.git ${BUILD_DIR}
pushd ${BUILD_DIR}
git checkout origin/split-kvstore ############ FIXME REMOVE LATER
git submodule update --init --recursive

cmake -DCMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX} -DWITH_JNI=ON .
make -j$(nproc)
$SUDO make install
popd
