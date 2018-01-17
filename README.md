We are in the process of separating CruzDB from the ZLog project (https://github.com/cruzdb/zlog),
migrating both under the umbrella of the https://github.com/cruzdb organization. Links, documentation, and other things may temporarily be out-of-date.

A little background:

* Key-Value Store: https://nwat.xyz/blog/2016/08/02/introduction-to-the-zlog-transaction-key-value-store/

## Build Notes

```
cmake -DZLOG_INSTALL_DIR=/../ .
make
```

## Build Status

[![Build Status](https://travis-ci.org/cruzdb/cruzdb.svg?branch=master)](https://travis-ci.org/cruzdb/cruzdb)
