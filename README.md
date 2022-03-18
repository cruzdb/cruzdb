# CruzDB

[![Build Status](https://travis-ci.org/cruzdb/cruzdb.svg?branch=master)](https://travis-ci.org/cruzdb/cruzdb)

---

See https://makedist.com/projects/cruzdb/ for more information.

I *love* working on CruzDB and plan to resume work sometime in the future. But for now I'm spending 100% of my time building a next-generation storage engine for modern hardware at https://github.com/redpanda-data/redpanda.

---

CruzDB is a key-value database that stores all its data in a single, high-performance distributed shared-log. The database uses multi-version concurrency control for managing transactions. Each transaction in CruzDB reads from an immutable snapshot of the database. When a transaction finishes, all of the information required to replay the transaction---a reference to the snapshot, and a record of the its reads and writes---are packaged into an object called an intention which is then appended to the log. Any node with access to the log can replay the intentions in the order that they appear in the log, and deterministically reconstruct the database for any past state.

In order to avoid forcing all database nodes from having to store an entire copy of the database, fine-grained materialized views of the are stored in the log. This allows database nodes to selectively cache database state, while still providing on-demand access to any subset of the database by reading from the log.

If you are thinking that this sounds a lot like Hyder, then you're right! Many components of Hyder can be matched to analagous components in CruzDB and CruzDB tends to use the same language. But the systems do have fundamental differences in how the database is structured and how conflict detection is implemented. In the coming weeks we'll be publishing more details about the design of the system, so please stay tuned.

A little background (but a lot has changed since this post):

* Key-Value Store: https://nwat.xyz/blog/2016/08/02/introduction-to-the-zlog-transaction-key-value-store/

# Build Notes

```
cmake -DZLOG_INSTALL_DIR=/../ .
make
```
