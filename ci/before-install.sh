#!/bin/bash

# adapted from https://github.com/ceph/ceph/blob/master/install-deps.sh

set -e
set -x

if [[ "$OSTYPE" == "darwin"* ]]; then
  brew update || true
  brew install git || true
  exit 0
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if test $(id -u) != 0 ; then
  SUDO=sudo
fi

function debs() {
  $SUDO apt-get update
  $SUDO env DEBIAN_FRONTEND=noninteractive \
    apt-get install -y git
}

function rpms() {
  yumdnf="yum"
  builddepcmd="yum-builddep -y"
  if command -v dnf > /dev/null; then
    yumdnf="dnf"
    $SUDO dnf install -y 'dnf-command(builddep)'
    builddepcmd="dnf -y builddep --allowerasing"
  fi
  $SUDO $yumdnf install -y git
}

source /etc/os-release
case $ID in
  debian|ubuntu)
    debs
    ;;

  centos|fedora)
    rpms
    ;;

  *)
    echo "$ID not supported. Install dependencies manually."
    exit 1
    ;;
esac
