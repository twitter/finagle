package com.twitter.finagle;

import org.junit.Test;

/**
 * A Java compilation test for Dtab manipulation.
 */
public class DtabCompilationTest {
  @Test
  public void testCompilation() {
    Dtab d = Dtab.emptyDtab();
    d = Dtab.local();
    d = Dtab.limited();
    d = Dtab.base();
    Dtab.setLocal(d);
    Dtab.setLimited(d);
    Dtab base = Dtab.base();
    Dtab.setBase(Dtab.emptyDtab());
    Dtab.setBase(base);
    d = Dtab.local().concat(Dtab.base());
    d = Dtab.local().append(Dentry.read("/foo=>/bar"));
    d = Dtab.limited().concat(Dtab.base());
    d = Dtab.limited().append(Dentry.read("/baz=>/foo"));
    Dentry dentry = new Dentry(Dentry.readPrefix("/s/*"), new NameTree.Leaf<Path>(Path.read("/a")));
  }
}
