#!/bin/bash

# adapted from https://github.com/ceph/ceph/blob/master/install-deps.sh

set -e
set -x

if [[ "$OSTYPE" == "darwin"* ]]; then
  brew update || true
  brew install boost protobuf cmake lmdb || true
  exit 0
fi

ZLOG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if test $(id -u) != 0 ; then
  SUDO=sudo
fi

function debs() {
  $SUDO apt-get update
  $SUDO apt-key update
  $SUDO env DEBIAN_FRONTEND=noninteractive \
    apt-get install -y cmake libprotobuf-dev \
      protobuf-compiler libboost-system-dev \
      libboost-program-options-dev lcov \
      default-jdk
}

function rpms() {
  yumdnf="yum"
  builddepcmd="yum-builddep -y"
  if command -v dnf > /dev/null; then
    yumdnf="dnf"
    $SUDO dnf install -y 'dnf-command(builddep)'
    builddepcmd="dnf -y builddep --allowerasing"
  fi

  $SUDO $yumdnf install -y redhat-lsb-core
  case $(lsb_release -si) in
    Fedora)
      if test $yumdnf = yum; then
        $SUDO $yumdnf install -y yum-utils
      fi
      ;;
    CentOS)
      $SUDO yum install -y yum-utils
      MAJOR_VERSION=$(lsb_release -rs | cut -f1 -d.)
      $SUDO yum-config-manager --add-repo https://dl.fedoraproject.org/pub/epel/$MAJOR_VERSION/x86_64/
      $SUDO yum install --nogpgcheck -y epel-release
      $SUDO rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-EPEL-$MAJOR_VERSION
      $SUDO rm -f /etc/yum.repos.d/dl.fedoraproject.org*
      if test $(lsb_release -si) = CentOS -a $MAJOR_VERSION = 7 ; then
        $SUDO yum-config-manager --enable cr
        case $(uname -m) in
          x86_64)
            $SUDO yum -y install centos-release-scl
            dts_ver=7
            ;;
        esac
      fi
      ;;
  esac

  extra="gcc-c++"
  if [ -n "$dts_ver" ]; then
    extra="devtoolset-7-gcc-c++ devtoolset-7-libatomic-devel"
  fi

  $SUDO $yumdnf install -y cmake boost-devel \
    protobuf-devel protobuf-compiler java-devel lcov \
    libatomic python-virtualenv ${extra}
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
