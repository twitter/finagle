package com.twitter.finagle

import com.twitter.util.{Return, Throw, Var}
import java.net.SocketAddress
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

case class TestAddr(arg: String) extends SocketAddress

class TestResolver extends Resolver {
  val scheme = "test"
  def bind(arg: String) = {
    val addr = Addr.Bound(TestAddr(arg))
    Var.value(addr)
  }
}

case class ConstResolver(a: Addr) extends Resolver {
  val scheme = "const"
  def bind(arg: String) = Var(a)
}

@RunWith(classOf[JUnitRunner])
class ResolverTest extends FunSuite {
  test("reject bad names") {
    intercept[ResolverAddressInvalid] { Resolver.eval("!foo!bar") }
  }

  test("reject unknown resolvers") {
    intercept[ResolverNotFoundException] { Resolver.eval("unknown!foobar") }
  }

  test("resolve ServiceLoaded resolvers") {
    val binding = Resolver.eval("test!xyz").bind()
    Var.sample(binding) match {
      case Addr.Bound(addrs) if addrs.size == 1 =>
        assert(addrs.head === TestAddr("xyz"))
      case _ => fail()
    }
  }

  test("get a resolver instance") {
    val Some(resolver) = Resolver.get(classOf[TestResolver])
    val binding = resolver.bind("xyz")
    Var.sample(binding) match {
      case Addr.Bound(addrs) if addrs.size == 1 =>
        assert(addrs.head === TestAddr("xyz"))
      case _ => fail()
    }
  }
  
  test("Resolver.resolve (backwards compat.)") {
    val exc = new Exception
    ConstResolver(Addr.Failed(exc)).resolve("blah") match {
      case Throw(`exc`) =>
      case _ => fail()
    }

    val sockaddr = new SocketAddress{}
    ConstResolver(Addr.Bound(sockaddr)).resolve("blah") match {
      case Return(g) => assert(g() === Set(sockaddr))
      case _ => fail()
    }
  }
}
