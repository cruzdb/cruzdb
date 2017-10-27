==========================
Key-Value Storage Engine
==========================

The ``cruzdb`` project provides a distributed key-value storage engine that
operates on top of the log service. The engine provides basic ``get``, ``put``,
``delete`` operations as well as support for snapshots, iterators, and
transactions. The database uses an MVCC scheme so queries over read-only
snapshots will not affect on-going transactions.

The database is created by provided a pointer to a log (see above for creating
a log).

.. code-block:: c++

	{
	  // open the database
	  auto db = DB::Open(log);
	
	  // run the optimistic transaction
	  auto txn = db.BeginTransaction();
	  txn.Put("67", "val");
	  txn.Commit();
	}

Multiple operations may be combined in a single transaction:

.. code-block:: c++

	// PUT: 94, 06
	auto txn = db.BeginTransaction();
	txn.Put("94", "val");
	txn.Put("06", "val");
	txn.Commit();

and transactions can contain any combination of ``get``, ``put``, and ``delete`` operations:

.. code-block:: c++

	...
	txn = db.BeginTransaction();
	txn.Put("93", "val");
	
	std::string data;
	txn.Get("88", &data);
	if (data == "foo")
	  txn.Delete("76");
	
	txn.Commit();
	...

## Iteration and Snapshots

The key-value store exposes an iterator and can operate on a snapshot of the
database. The following code snippet shows how to print all key-value pairs in
the database at the time the iterator is created (any modification that occur
after the iterator is created will not seen).

.. code-block:: c++

	auto it = db.NewIterator();
	while (it.Valid()) {
	  std::cout << it.key() << " " it.value() << std::endl;
	  it.Next();
	}

By default the ``NewIterator`` method will access the latest version of the
database. A snapshot can be taken and later read by an iterator:

.. code-block:: c++

	auto ss = db.GetSnapshot();
	
	// modify db ...
	
	// this iterator will not observe the previous modifications
	auto it = db.NewIterator(ss);
	while (it.Valid()) {
	  std::cout << it.key() << " " it.value() << std::endl;
	  it.Next();
	}

Here is a summary of the full iterator interface. I've removed the comments as
the method names should be self-explanatory:
 
.. code-block:: c++

	class Iterator {
	 public:
	  Iterator(Snapshot snapshot);
	
	  // true iff iterator points to a valid entry
	  bool Valid() const;
	
	  // reposition the iterator
	  void SeekToFirst();
	  void SeekToLast();
	  void Seek(const std::string& target);
	
	  // navigation
	  void Next();
	  void Prev();
	
	  // retrieve the current key-value entry
	  std::string key() const;
	  std::string value() const;
	
	  int status() const;
	
	  ...
	};
