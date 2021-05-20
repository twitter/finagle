package com.twitter.finagle

import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.AssertionsForJUnit

class DtabTest extends AnyFunSuite with AssertionsForJUnit {

  def pathTree(t: String) =
    NameTree.read(t).map(Name(_))

  def assertEquiv[T: Equiv](left: T, right: T): Assertion =
    assert(Equiv[T].equiv(left, right), s"$left!=$right")

  test("d1 ++ d2") {
    val d1 = Dtab.read("/foo => /bar")
    val d2 = Dtab.read("/foo=>/biz;/biz=>/$/inet/8080;/bar=>/$/inet/9090")

    assert(d1 ++ d2 == Dtab.read("""
      /foo=>/bar;
      /foo=>/biz;
      /biz=>/$/inet/8080;
      /bar=>/$/inet/9090
    """))
  }

  test("Dtab.read ignores comment lines with #") {
    val withComments = Dtab.read("""
# a comment
      /#foo => /biz  # another comment
             | ( /bliz & # yet another comment
                 /bluth ) # duh bluths
             ; #finalmente
      #/ignore=>/me;
    """)
    assert(
      withComments == Dtab(
        IndexedSeq(
          Dentry(
            Path.Utf8("#foo"),
            NameTree.Alt(
              NameTree.Leaf(Path.Utf8("biz")),
              NameTree.Union(
                NameTree
                  .Weighted(NameTree.Weighted.defaultWeight, NameTree.Leaf(Path.Utf8("bliz"))),
                NameTree
                  .Weighted(NameTree.Weighted.defaultWeight, NameTree.Leaf(Path.Utf8("bluth")))
              )
            )
          )
        )
      )
    )
  }

  test("d1 ++ Dtab.empty") {
    val d1 = Dtab.read("/foo=>/bar;/biz=>/baz")

    assert(d1 ++ Dtab.empty == d1)
  }

  test("Dtab.stripPrefix") {
    val d1, d2 = Dtab.read("/foo=>/bar;/baz=>/xxx/yyy")

    assert(d1.stripPrefix(d1).isEmpty)
    assert(d1.stripPrefix(d2).isEmpty)

    assertEquiv((d1 + Dentry.read("/foo => /123")).stripPrefix(d1), Dtab.read("/foo=>/123"))

    assertEquiv(d1.stripPrefix(d1 + Dentry.read("/s => /b")), d1)
    assert(Dtab.empty.stripPrefix(d1).isEmpty)
  }

  // These are mostly just compilation tests.
  test("Dtab is a Scala collection") {
    val b = Dtab.newBuilder
    b += Dentry.read("/a => /b")
    b += Dentry.read("/c => /d")
    val dtab = b.result

    val dtab1: Dtab = Dtab(dtab.map((e: Dentry) =>
      Dentry.read("%s=>%s".format(e.prefix.show.toUpperCase, e.dst.show.toUpperCase))))

    assert(dtab1.size == 2)
    dtab1(0) match {
      case Dentry(a, b) =>
        assert(a == Dentry.Prefix(Dentry.Prefix.Label("A")))
        assert(b == NameTree.Leaf(Path.Utf8("B")))
    }
  }

  test("Allows trailing semicolon") {
    val dtab =
      try {
        Dtab.read("""
          /b => /c;
          /a => /b;
          """)
      } catch { case _: IllegalArgumentException => Dtab.empty }
    assert(dtab.length == 2)
  }

  test("dtab rewrites with wildcards") {
    val dtab = Dtab.read("/a/*/c => /d")
    assert(dtab.lookup(Path.read("/a/b/c/e/f")) == NameTree.Leaf(Name.Path(Path.read("/d/e/f"))))
  }

  test("Set Dtab.limited") {
    assert(Dtab.limited.isEmpty)
    Dtab.unwind {
      Dtab.limited = Dtab.read("/a/*/c => /d")
      assert(Dtab.limited.show == "/a/*/c=>/d")
    }
    assert(Dtab.limited.isEmpty)
  }

  test("Set Dtab.limited - Java Api") {
    assert(Dtab.limited.isEmpty)
    Dtab.unwind {
      Dtab.setLimited(Dtab.read("/a/*/c => /d"))
      assert(Dtab.limited.show == "/a/*/c=>/d")
    }
    assert(Dtab.limited.isEmpty)
  }

  test("Dtab.unwind restores state for both local, limited") {
    val dtab1 = "/a/*/c=>/d"
    val n1 = Dtab.read(dtab1)

    assert(Dtab.limited.isEmpty)
    assert(Dtab.local.isEmpty)
    Dtab.unwind {
      Dtab.limited = n1
      Dtab.local = n1
      assert(Dtab.limited.show == dtab1)
      assert(Dtab.local.show == dtab1)
    }
    assert(Dtab.limited.isEmpty)
    assert(Dtab.local.isEmpty)
  }

  test("Dtab.limited and Dtab.local do not interfere with one another") {
    val dtab1 = "/a/*/c=>/d"
    val dtab2 = "/a/*/c=>/e"
    val n1 = Dtab.read(dtab1)
    val n2 = Dtab.read(dtab2)

    assert(Dtab.limited.isEmpty)
    assert(Dtab.local.isEmpty)
    Dtab.unwind {
      Dtab.limited = n1
      assert(Dtab.limited.show == dtab1)
      assert(Dtab.local.isEmpty)
      Dtab.local = n2
      assert(Dtab.limited.show == dtab1)
      assert(Dtab.local.show == dtab2)
    }
    assert(Dtab.limited.isEmpty)
    assert(Dtab.local.isEmpty)
  }
}
