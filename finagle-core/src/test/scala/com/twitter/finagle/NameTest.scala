package com.twitter.finagle

import com.twitter.util.{Return, Throw, Var}
import java.net.SocketAddress
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NameTest extends FunSuite {
  test("Name.fromGroup") {
    val g = Group.mutable[SocketAddress]()
    val n = Name.fromGroup(g)

    var addr: Addr = Addr.Pending
    n.bind() observe { addr = _ }
    val Addr.Bound(s1) = addr
    assert(s1.isEmpty)
    val set = Set(new SocketAddress{}, new SocketAddress{})
    g() = set
    val Addr.Bound(s2) = addr
    assert(s2 === set)
  }
}
