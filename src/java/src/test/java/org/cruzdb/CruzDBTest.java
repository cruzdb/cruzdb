package org.cruzdb;

import static org.junit.Assert.*;
import static org.assertj.core.api.Assertions.assertThat;
import java.util.Random;
import java.util.HashMap;
import org.junit.*;
import org.cruzdb.zlog.Log;
import org.cruzdb.zlog.LogException;

public class CruzDBTest {

  @Test
  public void dbOpen() throws LogException, CruzDBException {
    Random rand = new Random();
    String logname = "" + rand.nextInt();

    HashMap<String, String> opts = new HashMap<String, String>();
    opts.put("path", "db");
    Log log = Log.open("lmdb", opts, logname);

    CruzDB db = CruzDB.open(log, true);
  }

  @Test
  public void put() throws LogException, CruzDBException {
    Random rand = new Random();
    String logname = "" + rand.nextInt();

    HashMap<String, String> opts = new HashMap<String, String>();
    opts.put("path", "db");
    Log log = Log.open("lmdb", opts, logname);

    CruzDB db = CruzDB.open(log, true);
    db.put("key1".getBytes(), "value".getBytes());
    assertArrayEquals(db.get("key1".getBytes()), "value".getBytes());

    db.delete("key1".getBytes());
  }

  @Test
  public void iterator() throws LogException, CruzDBException {
    Random rand = new Random();
    String logname = "" + rand.nextInt();

    HashMap<String, String> opts = new HashMap<String, String>();
    opts.put("path", "db");
    Log log = Log.open("lmdb", opts, logname);

    CruzDB db = CruzDB.open(log, true);
    db.put("key1".getBytes(), "value1".getBytes());
    db.put("key2".getBytes(), "value2".getBytes());

    CruzIterator iterator = db.newIterator();
    iterator.seekToFirst();
    assertThat(iterator.isValid()).isTrue();
    assertThat(iterator.key()).isEqualTo("key1".getBytes());
    assertThat(iterator.value()).isEqualTo("value1".getBytes());
    iterator.next();
    assertThat(iterator.isValid()).isTrue();
    assertThat(iterator.key()).isEqualTo("key2".getBytes());
    assertThat(iterator.value()).isEqualTo("value2".getBytes());
    iterator.next();
    assertThat(iterator.isValid()).isFalse();
    iterator.seekToLast();
    iterator.prev();
    assertThat(iterator.isValid()).isTrue();
    assertThat(iterator.key()).isEqualTo("key1".getBytes());
    assertThat(iterator.value()).isEqualTo("value1".getBytes());
    iterator.seekToFirst();
    iterator.seekToLast();
    assertThat(iterator.isValid()).isTrue();
    assertThat(iterator.key()).isEqualTo("key2".getBytes());
    assertThat(iterator.value()).isEqualTo("value2".getBytes());
    iterator.next();
    assertThat(iterator.isValid()).isFalse();
  }

  @Test
  public void txn() throws LogException, CruzDBException {
    Random rand = new Random();
    String logname = "" + rand.nextInt();

    HashMap<String, String> opts = new HashMap<String, String>();
    opts.put("path", "db");
    Log log = Log.open("lmdb", opts, logname);

    CruzDB db = CruzDB.open(log, true);
    db.put("key1".getBytes(), "value1".getBytes());
    db.put("key2".getBytes(), "value2".getBytes());

    Transaction txn = db.newTransaction();
    byte[] value = txn.get("key1".getBytes());
    txn.commit();
  }
}
