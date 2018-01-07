CruzDB is part of an ecosystem that includes ZLog
(https://github.com/noahdesu/zlog). We are in the process of separating the two
projects and migrating both under the umbrella of the https://github.com/cruzdb
organization. Links and other things may temporarily be out-of-date.

A little background:

* Key-Value Store: https://nwat.io/blog/2016/08/02/introduction-to-the-zlog-transaction-key-value-store/

## Build Notes

```
cmake -DZLOG_INSTALL_DIR=/../ .
make
```

## Build Status

[![Build Status](https://travis-ci.org/cruzdb/cruzdb.svg?branch=master)](https://travis-ci.org/cruzdb/cruzdb)

| Distribution     | Status |
| ------------     | ------ |
| CentOS 7         | [![status](https://badges.herokuapp.com/travis/cruzdb/cruzdb?env=DOCKER_IMAGE=centos:7&label=centos:7)](https://travis-ci.org/cruzdb/cruzdb) |
| Debian Jessie    | [![status](https://badges.herokuapp.com/travis/cruzdb/cruzdb?env=DOCKER_IMAGE=debian:jessie&label=debian:jessie)](https://travis-ci.org/cruzdb/cruzdb) |
| Ubuntu 14.04 LTS | [![status](https://badges.herokuapp.com/travis/cruzdb/cruzdb?env=DOCKER_IMAGE=ubuntu:trusty&label=ubuntu:trusty)](https://travis-ci.org/cruzdb/cruzdb) |
| Ubuntu 16.04 LTS | [![status](https://badges.herokuapp.com/travis/cruzdb/cruzdb?env=DOCKER_IMAGE=ubuntu:xenial&label=ubuntu:xenial)](https://travis-ci.org/cruzdb/cruzdb) |
| Ubuntu 16.10     | [![status](https://badges.herokuapp.com/travis/cruzdb/cruzdb?env=DOCKER_IMAGE=ubuntu:yakkety&label=ubuntu:yakkety)](https://travis-ci.org/cruzdb/cruzdb) |
| Ubuntu 17.04     | [![status](https://badges.herokuapp.com/travis/cruzdb/cruzdb?env=DOCKER_IMAGE=ubuntu:zesty+RUN_COVERAGE=0&label=ubuntu:zesty)](https://travis-ci.org/cruzdb/cruzdb) |
| Fedora 24        | [![status](https://badges.herokuapp.com/travis/cruzdb/cruzdb?env=DOCKER_IMAGE=fedora:24&label=fedora:24)](https://travis-ci.org/cruzdb/cruzdb) |
| Fedora 25        | [![status](https://badges.herokuapp.com/travis/cruzdb/cruzdb?env=DOCKER_IMAGE=fedora:25&label=fedora:25)](https://travis-ci.org/cruzdb/cruzdb) |
| Fedora 26        | [![status](https://badges.herokuapp.com/travis/cruzdb/cruzdb?env=DOCKER_IMAGE=fedora:26&label=fedora:26)](https://travis-ci.org/cruzdb/cruzdb) |
| macOS            | [![status](https://badges.herokuapp.com/travis/cruzdb/cruzdb?env=OSX_BUILD=1&label=macOS)](https://travis-ci.org/cruzdb/cruzdb) |
