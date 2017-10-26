package org.cruzdb;

public class Transaction extends CruzObject {
  final CruzDB db;

  @Override
  protected void disposeInternal() {
    synchronized (this) {
      assert(isInitialized());
      disposeInternal(nativeHandle_);
    }
  }

  Transaction(CruzDB db, long nativeHandle) {
    super(nativeHandle);
    this.db = db;
  }

  /**
   * @param key the key of the entry.
   * @param value the value associated with the key.
   * @throws org.cruzdb.CruzDBException if an error occurs in the native library.
   */
  public void put(final byte[] key, final byte[] value) throws CruzDBException {
    put(nativeHandle_, key, 0, key.length, value, 0, value.length);
  }

  /**
   * @param key the key of the entry.
   * @return the value associated with the key.
   * @throws org.cruzdb.CruzDBException if an error occurs in the native library.
   */
  public byte[] get(final byte[] key) throws CruzDBException {
    return get(nativeHandle_, key, 0, key.length);
  }

  /**
   * @param key the key to delete.
   * @throws org.cruzdb.CruzDBException if an error occurs in the native library.
   */
  public void delete(final byte[] key) throws CruzDBException {
    delete(nativeHandle_, key, 0, key.length);
  }

  /**
   * @throws org.cruzdb.CruzDBException if an error occurs in the native library.
   */
  public void commit() throws CruzDBException {
    commit(nativeHandle_);
  }

  /**
   * @throws org.cruzdb.CruzDBException if an error occurs in the native library.
   */
  public void abort() throws CruzDBException {
    abort(nativeHandle_);
  }

  private native void disposeInternal(long handle);
  private native void put(long handle, byte[] key, int keyOffset, int keyLength,
      byte[] value, int valueOffset, int valueLength) throws CruzDBException;
  private native byte[] get(long handle, byte[] key, int keyOffset,
      int keyLength) throws CruzDBException;
  private native void delete(long handle, byte[] key, int keyOffset,
      int keyLength) throws CruzDBException;
  private native void commit(long handle) throws CruzDBException;
  private native void abort(long handle) throws CruzDBException;
}
