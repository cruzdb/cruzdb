package org.cruzdb;

import com.cruzdb.Log;

import java.util.Random;

import static org.junit.Assert.*;
import org.junit.*;
import static org.assertj.core.api.Assertions.assertThat;

public class LogTest {

  @Test
  public void dbOpen() throws LogException, com.cruzdb.LogException {
    Random rand = new Random();
    String logname = "" + rand.nextInt();
    Log log = Log.openLMDB(logname);

    DB db = DB.open(log, true);
  }

  @Test
  public void put() throws LogException, com.cruzdb.LogException {
    Random rand = new Random();
    String logname = "" + rand.nextInt();
    Log log = Log.openLMDB(logname);

    DB db = DB.open(log, true);
    db.put("key1".getBytes(), "value".getBytes());
    assertArrayEquals(db.get("key1".getBytes()), "value".getBytes());

    db.delete("key1".getBytes());
  }

  @Test
  public void iterator() throws LogException, com.cruzdb.LogException {
    Random rand = new Random();
    String logname = "" + rand.nextInt();
    Log log = Log.openLMDB(logname);

    DB db = DB.open(log, true);
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
  public void txn() throws LogException, com.cruzdb.LogException {
    Random rand = new Random();
    String logname = "" + rand.nextInt();
    Log log = Log.openLMDB(logname);

    DB db = DB.open(log, true);
    db.put("key1".getBytes(), "value1".getBytes());
    db.put("key2".getBytes(), "value2".getBytes());

    Transaction txn = db.newTransaction();
    byte[] value = txn.get("key1".getBytes());
    txn.commit();
  }
}
