package com.twitter.finagle;

import org.junit.Test;

/**
 * A Java compilation test for Dtab manipulation.
 */
public class DtabCompilationTest {
  @Test
  public void testCompilation() {
    Dtab d = Dtab.empty();
    d = Dtab.local();
    d = Dtab.base();
    Dtab.setLocal(d);
    Dtab base = Dtab.base();
    Dtab.setBase(Dtab.empty());
    Dtab.setBase(base);
    d = Dtab.local().concat(Dtab.base());
    d = Dtab.local().append(Dentry.read("/foo=>/bar"));
    Dentry dentry = new Dentry(Dentry.readPrefix("/s/*"), new NameTree.Leaf<Path>(Path.read("/a")));
  }
}
