package com.twitter.finagle.naming

import com.twitter.finagle._
import com.twitter.finagle.Namer.AddrWeightKey
import com.twitter.util.{Activity, Var}
import org.scalatest.funsuite.AnyFunSuite

class testnamer extends Namer {
  override def lookup(path: Path) =
    Activity.value(NameTree.Leaf(Name.Path(Path.read("/rewritten/by/test/namer"))))
}

class DefaultInterpreterTest extends AnyFunSuite {

  def assertEval(dtab: Dtab, path: String, expected: Name.Bound*): Unit = {
    DefaultInterpreter.bind(dtab, Path.read(path)).sample().eval match {
      case Some(actual) => assert(actual.map(_.addr.sample) == expected.map(_.addr.sample).toSet)
      case _ => assert(false)
    }
  }

  def boundWithWeight(weight: Double, addrs: Address*): Name.Bound =
    Name.Bound(
      Var.value(Addr.Bound(addrs.toSet, Addr.Metadata(AddrWeightKey -> weight))),
      addrs.toSet
    )

  test("basic dtab evaluation") {
    val dtab = Dtab.read("/foo=>/$/inet/8080")
    assertEval(dtab, "/foo", Name.bound(Address(8080)))
  }

  test("with indirections") {
    val dtab = Dtab.read("/foo=>/bar;/bar=>/$/inet/8080")
    assertEval(dtab, "/foo", Name.bound(Address(8080)))
  }

  test("order of dtab evaluation") {
    val d1 = Dtab.read("/foo=>/bar")
    val d2 = Dtab.read("/foo=>/biz;/biz=>/$/inet/8080;/bar=>/$/inet/9090")

    assertEval(d1 ++ d2, "/foo", Name.bound(Address(8080)))
    assertEval(d2 ++ d1, "/foo", Name.bound(Address(9090)))
  }

  test("recurse back to the dtab") {
    val dtab = Dtab.read(
      "/foo=>/$/com.twitter.finagle.naming.testnamer;/rewritten/by/test/namer=>/$/inet/7070"
    )

    assertEval(dtab, "/foo", Name.bound(Address(7070)))
  }

  test("full example") {
    val dtab = Dtab.read("""
      /foo => /bar;
      /foo => 3 * /baz & 2 * /booz & /$/com.twitter.finagle.naming.testnamer;
      /rewritten/by/test/namer => /$/inet/7070;
      /baz => 3 * /$/inet/8080 & 2 * /$/inet/9090;
      /booz => ~
    """)

    assertEval(
      dtab,
      "/foo",
      boundWithWeight(3.0, Address(8080)),
      boundWithWeight(2.0, Address(9090)),
      boundWithWeight(1.0, Address(7070))
    )
  }
}
