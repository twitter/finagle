package com.twitter.finagle;

import org.junit.Test;

/**
 * A Java compilation test for Dtab manipulation.
 */
public class DtabCompilationTest {
  @Test
  public void testCompilation() {
    Dtab d = Dtabs.empty();
    d = Dtabs.local();
    d = Dtabs.base();
    Dtabs.setLocal(d);
    Dtab base = Dtabs.base();
    Dtabs.setBase(Dtabs.empty());
    Dtabs.setBase(base);
    d = Dtabs.local().concat(Dtabs.base());
    d = Dtabs.local().append(Dentry.read("/foo=>/bar"));
    Dentry dentry = new Dentry(Dentry.readPrefix("/s/*"), new NameTree.Leaf<Path>(Path.read("/a")));
  }
}
