package com.twitter.finagle

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}

@RunWith(classOf[JUnitRunner])
class NameTreeParsersTest extends FunSuite with AssertionsForJUnit {
  test("parsePath") {
    assert(NameTreeParsers.parsePath("/") == Path.empty)
    assert(NameTreeParsers.parsePath("  /foo/bar  ") == Path.Utf8("foo", "bar"))
    assert(NameTreeParsers.parsePath("/\\x66\\x6f\\x6F") == Path.Utf8("foo"))

    intercept[IllegalArgumentException] { NameTreeParsers.parsePath("") }
    intercept[IllegalArgumentException] { NameTreeParsers.parsePath("/foo/bar/") }
    intercept[IllegalArgumentException] { NameTreeParsers.parsePath("/{}") }
    intercept[IllegalArgumentException] { NameTreeParsers.parsePath("/\\?") }
    intercept[IllegalArgumentException] { NameTreeParsers.parsePath("/\\x?") }
    intercept[IllegalArgumentException] { NameTreeParsers.parsePath("/\\x0?") }
  }

  test("error messages") {
    assert(
      intercept[IllegalArgumentException] { NameTreeParsers.parsePath("/foo^bar") }
        .getMessage contains "'/foo[^]bar'")
    assert(
      intercept[IllegalArgumentException] { NameTreeParsers.parsePath("/foo/bar/") }
        .getMessage contains "'/foo/bar/[]'")
  }

  test("parseNameTree") {
    val defaultWeight = NameTree.Weighted.defaultWeight

    assert(NameTreeParsers.parseNameTree("! | ~ | $") == NameTree.Alt(NameTree.Fail, NameTree.Neg, NameTree.Empty))
    assert(NameTreeParsers.parseNameTree("/foo/bar") == NameTree.Leaf(Path.Utf8("foo", "bar")))
    assert(NameTreeParsers.parseNameTree("  /foo & /bar  ") ==
      NameTree.Union(
        NameTree.Weighted(defaultWeight, NameTree.Leaf(Path.Utf8("foo"))),
        NameTree.Weighted(defaultWeight, NameTree.Leaf(Path.Utf8("bar")))))
    assert(NameTreeParsers.parseNameTree("  /foo | /bar  ") ==
      NameTree.Alt(NameTree.Leaf(Path.Utf8("foo")), NameTree.Leaf(Path.Utf8("bar"))))
    assert(NameTreeParsers.parseNameTree("/foo & /bar | /bar & /baz") ==
      NameTree.Alt(
        NameTree.Union(
          NameTree.Weighted(defaultWeight, NameTree.Leaf(Path.Utf8("foo"))),
          NameTree.Weighted(defaultWeight, NameTree.Leaf(Path.Utf8("bar")))),
        NameTree.Union(
          NameTree.Weighted(defaultWeight, NameTree.Leaf(Path.Utf8("bar"))),
          NameTree.Weighted(defaultWeight, NameTree.Leaf(Path.Utf8("baz"))))))

    assert(NameTreeParsers.parseNameTree("1 * /foo & 2 * /bar | .5 * /bar & .5 * /baz") ==
      NameTree.Alt(
        NameTree.Union(
          NameTree.Weighted(1D, NameTree.Leaf(Path.Utf8("foo"))),
          NameTree.Weighted(2D, NameTree.Leaf(Path.Utf8("bar")))),
        NameTree.Union(
          NameTree.Weighted(0.5D, NameTree.Leaf(Path.Utf8("bar"))),
          NameTree.Weighted(0.5D, NameTree.Leaf(Path.Utf8("baz"))))))

    intercept[IllegalArgumentException] { NameTreeParsers.parseNameTree("") }
    intercept[IllegalArgumentException] { NameTreeParsers.parseNameTree("#") }
    intercept[IllegalArgumentException] { NameTreeParsers.parseNameTree("/foo &") }
    intercept[IllegalArgumentException] { NameTreeParsers.parseNameTree("/foo & 0.1.2 * /bar")}
    intercept[IllegalArgumentException] { NameTreeParsers.parseNameTree("/foo & . * /bar")}
  }

  test("parseDentry") {
    assert(NameTreeParsers.parseDentry("/=>!") == Dentry(Path.empty, NameTree.Fail))
    assert(NameTreeParsers.parseDentry("/ => !") == Dentry(Path.empty, NameTree.Fail))

    intercept[IllegalArgumentException] { NameTreeParsers.parseDentry("/&!") }
  }

  test("parseDtab") {
    assert(NameTreeParsers.parseDtab("") == Dtab.empty)
    assert(NameTreeParsers.parseDtab("  /=>!  ") == Dtab(IndexedSeq(Dentry(Path.empty, NameTree.Fail))))
    assert(NameTreeParsers.parseDtab("/=>!;") == Dtab(IndexedSeq(Dentry(Path.empty, NameTree.Fail))))
    assert(NameTreeParsers.parseDtab("/=>!;/foo=>/bar") ==
      Dtab(IndexedSeq(
        Dentry(Path.empty, NameTree.Fail),
        Dentry(Path.Utf8("foo"), NameTree.Leaf(Path.Utf8("bar"))))))
  }
}
