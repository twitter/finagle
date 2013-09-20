package com.twitter.finagle

import com.twitter.util.Return
import java.net.SocketAddress
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

case class TestGroup(addr: String) extends Group[SocketAddress] {
  def members = Set()
}

class TestResolver extends Resolver {
  val scheme = "test"
  def resolve(addr: String) = Return(TestGroup(addr))
}

@RunWith(classOf[JUnitRunner])
class ResolverTest extends FunSuite {
  test("reject bad names") {
    assert(Resolver.resolve("!foo!bar").isThrow)
  }

  test("reject unknown resolvers") {
    assert(Resolver.resolve("unknown!foobar").isThrow)
  }

  test("resolve ServiceLoaded resolvers") {
    Resolver.resolve("test!xyz")() match {
      case p: Proxy => assert(p.self === TestGroup("xyz"))
      case _ => assert(false)
    }
  }

  test("assign names") {
    Resolver.resolve("test!xyz")() match {
      case NamedGroup("test!xyz") =>
      case _ => assert(false)
    }

    Resolver.resolve("myname=test!xyz")() match {
      case NamedGroup("myname") =>
      case _ => assert(false)
    }
  }

  test("provides a resolutions set") {
    Resolver.resolve("test!xyz")()
    assert(Resolver.resolutions == Set(List("test!xyz")))
  }

  test("get a resolver instance") {
    val group = Resolver.get(classOf[TestResolver]).get.resolve("xyz")()
    assert(group === TestGroup("xyz"))
  }

  // names
}
