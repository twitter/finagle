package com.twitter.finagle

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}

@RunWith(classOf[JUnitRunner])
class NameTreeParsersTest extends FunSuite with AssertionsForJUnit {
  test("parsePath") {
    assert(NameTreeParsers.parsePath("/") === Path.empty)
    assert(NameTreeParsers.parsePath("  /foo/bar  ") === Path.Utf8("foo", "bar"))
    assert(NameTreeParsers.parsePath("/\\x66\\x6f\\x6F") === Path.Utf8("foo"))

    intercept[IllegalArgumentException] { NameTreeParsers.parsePath("") }
    intercept[IllegalArgumentException] { NameTreeParsers.parsePath("/foo/bar/") }
    intercept[IllegalArgumentException] { NameTreeParsers.parsePath("/{}") }
    intercept[IllegalArgumentException] { NameTreeParsers.parsePath("/\\?") }
    intercept[IllegalArgumentException] { NameTreeParsers.parsePath("/\\x?") }
    intercept[IllegalArgumentException] { NameTreeParsers.parsePath("/\\x0?") }
  }

  test("parseNameTree") {
    assert(NameTreeParsers.parseNameTree("! | ~ | $") === NameTree.Alt(NameTree.Fail, NameTree.Neg, NameTree.Empty))
    assert(NameTreeParsers.parseNameTree("/foo/bar") === NameTree.Leaf(Path.Utf8("foo", "bar")))
    assert(NameTreeParsers.parseNameTree("  /foo & /bar  ") ===
      NameTree.Union(NameTree.Leaf(Path.Utf8("foo")), NameTree.Leaf(Path.Utf8("bar"))))
    assert(NameTreeParsers.parseNameTree("  /foo | /bar  ") ===
      NameTree.Alt(NameTree.Leaf(Path.Utf8("foo")), NameTree.Leaf(Path.Utf8("bar"))))
    assert(NameTreeParsers.parseNameTree("/foo & /bar | /bar & /baz") ===
      NameTree.Alt(
        NameTree.Union(NameTree.Leaf(Path.Utf8("foo")), NameTree.Leaf(Path.Utf8("bar"))),
        NameTree.Union(NameTree.Leaf(Path.Utf8("bar")), NameTree.Leaf(Path.Utf8("baz")))))

    intercept[IllegalArgumentException] { NameTreeParsers.parseNameTree("") }
    intercept[IllegalArgumentException] { NameTreeParsers.parseNameTree("#") }
    intercept[IllegalArgumentException] { NameTreeParsers.parseNameTree("/foo &") }
  }

  test("parseDentry") {
    assert(NameTreeParsers.parseDentry("/=>!") === Dentry(Path.empty, NameTree.Fail))
    assert(NameTreeParsers.parseDentry("/ => !") === Dentry(Path.empty, NameTree.Fail))

    intercept[IllegalArgumentException] { NameTreeParsers.parseDentry("/&!") }
  }

  test("parseDtab") {
    assert(NameTreeParsers.parseDtab("") === Dtab.empty)
    assert(NameTreeParsers.parseDtab("  /=>!  ") === Dtab(IndexedSeq(Dentry(Path.empty, NameTree.Fail))))
    assert(NameTreeParsers.parseDtab("/=>!;") === Dtab(IndexedSeq(Dentry(Path.empty, NameTree.Fail))))
    assert(NameTreeParsers.parseDtab("/=>!;/foo=>/bar") ===
      Dtab(IndexedSeq(
        Dentry(Path.empty, NameTree.Fail),
        Dentry(Path.Utf8("foo"), NameTree.Leaf(Path.Utf8("bar"))))))
  }
}
