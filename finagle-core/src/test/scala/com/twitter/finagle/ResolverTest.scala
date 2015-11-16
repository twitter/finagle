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
    val Name.Bound(addr) = Resolver.eval("test!xyz")
    Var.sample(addr) match {
      case Addr.Bound(addrs, attrs) if addrs.size == 1 && attrs.isEmpty =>
        assert(addrs.head == TestAddr("xyz"))
      case _ => fail()
    }
  }

  test("get a resolver instance") {
    val Some(resolver) = Resolver.get(classOf[TestResolver])
    val binding = resolver.bind("xyz")
    Var.sample(binding) match {
      case Addr.Bound(addrs, attrs) if addrs.size == 1 && attrs.isEmpty =>
        assert(addrs.head == TestAddr("xyz"))
      case _ => fail()
    }
  }

  test("Resolver.resolve (backwards compat.)") {
    val exc = new Exception
    ConstResolver(Addr.Failed(exc)).resolve("blah") match {
      case Throw(`exc`) =>
      case _ => fail()
    }

    val sockaddr = new SocketAddress {}
    ConstResolver(Addr.Bound(sockaddr)).resolve("blah") match {
      case Return(g) => assert(g() == Set(sockaddr))
      case _ => fail()
    }
  }

  test("Resolver.evalLabeled: Resolve labels of labeled addresses") {
    val label = "foo"
    val binding = Resolver.evalLabeled(label + "=test!xyz")
    assert(binding._2 == label)
  }

  test("Resolver.evalLabeled: Resolve empty string as label for unlabeled addresses") {
    val binding = Resolver.evalLabeled("test!xyz")
    assert(binding._2.isEmpty)
  }

  test("Return equatable names") {
    assert(Resolver.eval("test!xyz") == Resolver.eval("test!xyz"))
    assert(Resolver.eval("test!xyz") != Resolver.eval("test!xxx"))
  }

  test("throw when registering multiple resolvers for the same scheme") {
    object TestResolver extends BaseResolver(() => Seq(new TestResolver, new TestResolver))

    intercept[MultipleResolversPerSchemeException] {
      TestResolver.get(classOf[TestResolver])
    }
  }
}
