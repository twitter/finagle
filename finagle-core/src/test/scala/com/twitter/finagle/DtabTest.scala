package com.twitter.finagle

import com.twitter.util.{Var, Updatable}
import java.net.{InetSocketAddress, SocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

case class DtabAddr(arg: String, v: Var[Addr] with Updatable[Addr]) 
extends SocketAddress {
  override def toString = "DtabAddr"
}

class DtabTestResolver extends Resolver {
  val scheme = "d"
  def bind(arg: String) = {
    val v = Var[Addr](Addr.Neg)
    v() = Addr.Bound(DtabAddr(arg, v))
    v
  }
}

@RunWith(classOf[JUnitRunner])
class DtabTest extends FunSuite {

  test("Dynamically resolve changes") {
    val d = Dtab.empty
      .delegated("/blah", "d!blah")
      .delegated("/blah/foo", "d!blahfoo")

    val va = d.bind("/blah")
    @volatile var _va: Addr = null
    va observe { _va = _ }
    val Addr.Bound(s) = _va
    assert(s.size === 1)
    val DtabAddr("blah", v) = s.head
    v() = Addr.Neg

    assert(_va === Addr.Neg)
    
    val sa = new SocketAddress{}
    v() = Addr.Bound(sa)

    val Addr.Bound(sockaddrs) = _va
    assert(sockaddrs === Set(sa))
  }
  
  test("Defer to pending") {
    val d = Dtab.empty
      .delegated("/blah", "d!blah")
      .delegated("/blah/foo", "d!blahfoo")

    val va = d.bind("/blah/foo")
    @volatile var _va: Addr = null
    va observe { _va = _ }
    val Addr.Bound(s) = _va
    assert(s.size === 1)
    val DtabAddr("blahfoo", v) = s.head

    v() = Addr.Pending

    assert(_va === Addr.Pending)
    
    val sa = new SocketAddress{}
    v() = Addr.Bound(sa)
    
    _va match {
      case Addr.Bound(set) => assert(set === Set(sa))
      case _ => fail()
    }
    
    v() = Addr.Neg
    
    _va match {
      case Addr.Bound(set) if set.size == 1 =>
        val PartialSocketAddress(DtabAddr("blah", _), "foo") = set.head
      case _ => fail()
    }
  }

  test("Handle recursive resolves, passes partial paths") {
    val d = Dtab.empty
      .delegated("/blah", "d!blah")
      .delegated("/blah/foo", "d!blahfoo")

    val va = d.bind("/blah/foo")
    @volatile var _va: Addr = null
    va observe { _va = _ }
    val Addr.Bound(s) = _va
    assert(s.size === 1)
    val DtabAddr("blahfoo", v2) = s.head
    
    v2() = Addr.Neg
    
    val Addr.Bound(s1) = _va
    assert(s1.size === 1)
    val PartialSocketAddress(DtabAddr("blah", v1), "foo") = s1.head
    
    v1() = Addr.Neg
    
    assert(_va === Addr.Neg)
    
    val sa = new SocketAddress{}
    v2() = Addr.Bound(sa)
    val Addr.Bound(s2) = _va
    assert(s2 === Set(sa))
  }

  test("Does not recurse indefinitely") {
    val d = Dtab.empty
      .delegated("/foo", "/bar")
      .delegated("/bar", "/foo")

    val va = d.bind("/foo")
    val Addr.Failed(exc) = va()
    assert(exc.getMessage() === "Resolution reached maximum depth")
  }
  
  test("Handles unknown") {
    val d = Dtab.empty
      .delegated("/foo", "/bar")
    
    assert(d.bind("/blah").apply() === Addr.Neg)
  }

  test("Dtab.bind nonexistent") {
    val d = Dtab.empty
      .delegated("/foo", "/bar")
      
    
    d.bind("/blah") match {
      case Var(Addr.Neg) =>
      case _ => fail()
    }
  }

  test("Dtab.delegated(Dtab)") {
    val d1 = Dtab.empty
      .delegated("/foo", "/bar")
    
    val d2 = Dtab.empty
      .delegated("/foo", "/biz")
      .delegated("/biz", "inet!:8080")
      .delegated("/bar", "inet!:9090")

    (d1 delegated d2).bind("/foo") match {
      case Var(Addr.Bound(s)) if s.size == 1 =>
        assert(s.head === new InetSocketAddress(8080))
      case _ => fail()
    }
    
    (d2 delegated d1).bind("/foo") match {
      case Var(Addr.Bound(s)) if s.size == 1 =>
        assert(s.head === new InetSocketAddress(9090))
      case _ => fail()
    }
  }
}
