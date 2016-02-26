package com.twitter.finagle

import com.twitter.util.{Witness, Var}
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
    n.addr.changes.register(Witness({ addr = _ }))
    assert(addr == Addr.Pending)
    val set = Set(new SocketAddress {}, new SocketAddress {})
    g() = set

    val Addr.Bound(s2, r) = addr
    assert(s2 == set)
    assert(r.isEmpty)
  }

  test("Name.Bound maintains equality as per 'id'") {
    val id1, id2 = new {}
    val a1, a2 = Var(Addr.Pending)

    assert(Name.Bound(a1, id1) == Name.Bound(a2, id1))
    assert(Name.Bound(a1, id1) != Name.Bound(a1, id2))

    // It sucks that this is not symmetric, oh well.
    assert(Name.Bound(a1, id1) == id1)
    assert(Name.Bound(a1, id1) != id2)
  }

  test("Name.all maintains equality") {
    val names = Seq.fill(10) { Name.Bound.singleton(Var(Addr.Pending)) }.toSet

    assert(Name.all(names) == Name.all(names))
    assert(Name.all(names) != Name.all(names drop 1))
  }
}
