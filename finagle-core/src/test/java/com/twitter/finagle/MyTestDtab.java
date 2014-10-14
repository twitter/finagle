package com.twitter.finagle;

/**
 * A Java compilation test for Dtab manipulation.
 */

public class MyTestDtab {
  static {
    Dtab d = Dtab.empty();
    d = Dtab.local();
    d = Dtab.base();
    Dtab.setLocal(d);
    Dtab base = Dtab.base();
    Dtab.setBase(Dtab.empty());
    Dtab.setBase(base);
    d = Dtab.local().concat(Dtab.base());
    d = Dtab.local().append(Dentry.read("/foo=>/bar"));
  }
}
