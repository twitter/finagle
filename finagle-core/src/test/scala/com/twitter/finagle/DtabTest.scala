package com.twitter.finagle

import com.twitter.util.{Var, Updatable}
import java.net.{InetSocketAddress, SocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DtabTest extends FunSuite {

  def assertEquiv[T: Equiv](left: T, right: T) = assert(
    if (Equiv[T].equiv(left, right)) None
    else Some(left+"!="+right)
  )

  test("Lookup all prefixes in reverse order") {
    val dtab = Dtab.read("/foo/bar=>/xxx;/foo=>/yyy")

    assertEquiv(
      dtab.lookup(Path.read("/foo/bar/baz")).sample(),
      NameTree.read("/yyy/bar/baz | /xxx/baz"))
  }

  test("Expand names") {
    val dtab = Dtab.read("/foo/bar => /xxx|/yyy&/zzz")

    assertEquiv(
      dtab.lookup(Path.read("/foo/bar/baz")).sample(),
      NameTree.read("/xxx/baz | /yyy/baz & /zzz/baz"))
  }

  test("d1 ++ d2") {
    val d1 = Dtab.read("/foo => /bar")
    val d2 = Dtab.read("/foo=>/biz;/biz=>/$/inet//8080;/bar=>/$/inet//9090")

    ((d1 ++ d2) orElse Namer.global).bindAndEval(NameTree.Leaf(Path.read("/foo"))).sample() match {
      case Addr.Bound(s) if s.size == 1 =>
        assert(s.head === new InetSocketAddress(8080))
      case addr => fail("Invalid address "+addr)
    }

    ((d2 ++ d1) orElse Namer.global).bindAndEval(NameTree.Leaf(Path.read("/foo"))).sample() match {
      case Addr.Bound(s) if s.size == 1 =>
        assert(s.head === new InetSocketAddress(9090))
      case _ => fail()
    }
  }

  test("Dtab.stripPrefix") {
    val d1, d2 = Dtab.read("/foo=>/bar;/baz=>/xxx/yyy")

    assert(d1.stripPrefix(d1).isEmpty)
    assert(d1.stripPrefix(d2).isEmpty)

    assertEquiv(
      (d1 + Dentry.read("/foo => /123")).stripPrefix(d1),
      Dtab.read("/foo=>/123"))

    assertEquiv(d1.stripPrefix(d1+ Dentry.read("/s => /b")), d1)
    assert(Dtab.empty.stripPrefix(d1).isEmpty)
  }

  // These are mostly just compilation tests.
  test("Dtab is a Scala collection") {
    val b = Dtab.newBuilder
    b += Dentry.read("/a => /b")
    b += Dentry.read("/c => /d")
    val dtab = b.result
    
    val dtab1: Dtab = dtab map { case Dentry(a, b) => 
      Dentry.read("%s=>%s".format(a.show.toUpperCase, b.show.toUpperCase))
    }

    assert(dtab1.size === 2)
    dtab1(0) match {
      case Dentry(a, b) =>
        assert(a === Path.Utf8("A"))
        assert(b === NameTree.Leaf(Path.Utf8("B")))
    }
  }
}
