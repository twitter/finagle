package com.twitter.finagle

import com.twitter.util.Future
import com.twitter.util.Time
import com.twitter.util.Var
import org.scalatest.funsuite.AnyFunSuite

object TestAddr {
  case class StringFactory(s: String) extends ServiceFactory[Any, String] {
    val svc = Service.const(Future.value(s))
    override def apply(conn: ClientConnection) = Future.value(svc)
    override def close(deadline: Time) = Future.Done
    def status: Status = svc.status
  }

  def apply(arg: String): Address = {
    Address[Any, String](StringFactory(arg))
  }
}

class TestResolver extends Resolver {
  val scheme = "test"
  def bind(arg: String) = {
    val addr = Addr.Bound(TestAddr(arg))
    Var.value(addr)
  }
}

class TestInetResolver extends Resolver {
  val scheme = "inet"
  def bind(arg: String) = {
    val addr = Addr.Bound(TestAddr(arg))
    Var.value(addr)
  }
}

case class ConstResolver(a: Addr) extends Resolver {
  val scheme = "const"
  def bind(arg: String) = Var(a)
}

class ResolverTest extends AnyFunSuite {
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

  test("supports a service loaded inet resolver") {
    val resolver = new BaseResolver(() => Seq(new TestInetResolver)) {}
    assert(resolver.eval("inet!xyz") == Resolver.eval("inet!xyz"))
  }
}
