package org.cruzdb;

import java.io.IOException;
import org.cruzdb.zlog.Log;

public class CruzDB extends CruzObject {

  // FIXME
  public static final int NOT_FOUND = -1;

  static {
    CruzDB.loadLibrary();
  }

  public static synchronized void loadLibrary() {
    String tmpDir = System.getenv("CRUZDB_SHAREDLIB_DIR");
    try {
      NativeLibraryLoader.getInstance().loadLibrary(tmpDir);
    } catch (IOException e) {
      throw new RuntimeException("Unable to load the CruzDB shared library: " + e);
    }
  }

  private CruzDB() {
    super();
  }

  @Override
  protected void disposeInternal() {
    synchronized (this) {
      assert(isInitialized());
      disposeInternal(nativeHandle_);
    }
  }

  /**
   * @param log the log used to store the database.
   * @param create create the log if it doesn't exist.
   * @return database instance.
   * @throws org.cruzdb.CruzDBException if an error occurs in the native library.
   */
  public static CruzDB open(Log log, boolean create) throws CruzDBException {
    CruzDB db = new CruzDB();
    db.openNative(log.handle(), create);
    return db;
  }

  /**
   * @param key the key to be inserted.
   * @param value the value associated with the key.
   * @throws org.cruzdb.CruzDBException if an error occurs in the native library.
   */
  public void put(final byte[] key, final byte[] value) throws CruzDBException {
    put(nativeHandle_, key, 0, key.length, value, 0, value.length);
  }

  /**
   * @param key the key to be inserted.
   * @param value the value associated with the key.
   * @return size of value.
   * @throws org.cruzdb.CruzDBException if an error occurs in the native library.
   */
  public int get(final byte[] key, final byte[] value) throws CruzDBException {
    return get(nativeHandle_, key, 0, key.length, value, 0, value.length);
  }

  /**
   * @param key the key to be inserted.
   * @return the value of the associated key.
   * @throws org.cruzdb.CruzDBException if an error occurs in the native library.
   */
  public byte[] get(final byte[] key) throws CruzDBException {
    return get(nativeHandle_, key, 0, key.length);
  }

  /**
   * @param key the key of the entry to be deleted.
   * @throws org.cruzdb.CruzDBException if an error occurs in the native library.
   */
  public void delete(final byte[] key) throws CruzDBException {
    delete(nativeHandle_, key, 0, key.length);
  }

  /**
   * @return a new iterator.
   */
  public CruzIterator newIterator() {
    return new CruzIterator(this, iterator(nativeHandle_));
  }

  /**
   * @return a new transaction.
   */
  public Transaction newTransaction() {
    return new Transaction(this, transaction(nativeHandle_));
  }

  private native void disposeInternal(long handle);
  private native void openNative(long logHandle, boolean create) throws CruzDBException;
  private native void put(long handle, byte[] key, int keyOffset, int keyLength,
      byte[] value, int valueOffset, int valueLength) throws CruzDBException;
  private native int get(long handle, byte[] key, int keyOffset, int keyLength,
      byte[] value, int valueOffset, int valueLength) throws CruzDBException;
  private native byte[] get(long handle, byte[] key, int keyOffset,
      int keyLength) throws CruzDBException;
  private native void delete(long handle, byte[] key, int keyOffset,
      int keyLength) throws CruzDBException;
  private native long iterator(long handle);
  private native long transaction(long handle);
}
