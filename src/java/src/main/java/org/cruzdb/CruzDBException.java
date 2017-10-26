package org.cruzdb;

/**
 * Signals that a CruzDB error has occurred.
 *
 * This exception is used to describe an internal error from the C++ CruzDB
 * library.
 */
public class CruzDBException extends Exception {
  public CruzDBException(final String msg) {
    super(msg);
  }
}
