package com.twitter.finagle.naming

import com.twitter.finagle._
import com.twitter.util.Activity
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class DefaultInterpreterTest extends FunSuite {

  def assertEval(dtab: Dtab, path: String, expected: Name.Bound*) {
    DefaultInterpreter.bind(dtab, Path.read(path)).sample().eval match {
      case Some(actual) => assert(actual.map(_.addr.sample) === expected.map(_.addr.sample).toSet)
      case _ => assert(false)
    }
  }

  test("basic dtab evaluation") {
    val dtab = Dtab.read("/foo=>/$/inet/0/8080")
    assertEval(dtab, "/foo", Name.bound(new InetSocketAddress(8080)))
  }

  test("with indirections") {
    val dtab = Dtab.read("/foo=>/bar;/bar=>/$/inet/0/8080")
    assertEval(dtab, "/foo", Name.bound(new InetSocketAddress(8080)))
  }

  test("order of dtab evaluation") {
    val d1 = Dtab.read("/foo=>/bar")
    val d2 = Dtab.read("/foo=>/biz;/biz=>/$/inet/0/8080;/bar=>/$/inet/0/9090")

    assertEval(d1 ++ d2, "/foo", Name.bound(new InetSocketAddress(8080)))
    assertEval(d2 ++ d1, "/foo", Name.bound(new InetSocketAddress(9090)))
  }

  test("full example") {
    val dtab = Dtab.read("""
      /foo => /bar;
      /foo => 3 * /baz & 2 * /booz;
      /baz => 3 * /$/inet/0/8080 & 2 * /$/inet/0/9090;
      /booz => ~
    """)

    assertEval(dtab, "/foo",
      Name.bound(new InetSocketAddress(8080)), Name.bound(new InetSocketAddress(9090)))
  }
}
