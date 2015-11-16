package com.twitter.finagle

import com.twitter.newZk
import com.twitter.finagle.{Addr, Resolver, Name, Namer}
import com.twitter.util.Var
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import org.scalatest.FunSuite
import scala.language.implicitConversions

@RunWith(classOf[JUnitRunner])
class ServersetNamerTest
  extends FunSuite
  with AssertionsForJUnit
{

  def mkNamer(f: String => Var[Addr]): Namer = new com.twitter.serverset {
    override protected[this] def resolve(spec: String) = f(spec)
  }

  def schemeOk(scheme: String): Unit = {
    val addr = Addr.Bound(new InetSocketAddress(7127))
    var named = 0
    val namer = mkNamer { spec =>
      assert(spec == s"$scheme!hosts!/twitter/service/role/env/job!endpoint")
      named += 1
      Var.value(addr)
    }
    assert(named == 0)

    val path = Path.read("/hosts/twitter/service/role/env/job:endpoint")
    namer.bind(NameTree.Leaf(path)).sample() match {
      case NameTree.Leaf(bound: Name.Bound) =>
        assert(named == 1)
        assert(bound.addr.sample() == addr)
        assert(bound.path == Path.empty)
        assert(bound.id == Path.Utf8(
          "$", "com.twitter.serverset",
          "hosts", "twitter", "service", "role", "env", "job:endpoint"))

      case _ => fail(s"invalid name: ${path.show}")
    }
  }

  test("binds to zk2") {
    newZk.let(true) { schemeOk("zk2") }
  }

  test("binds to zk") {
    newZk.let(false) { schemeOk("zk") }
  }

  test("negative resolution") {
    newZk.let(true) {
      var named = 0
      val namer = mkNamer { spec =>
        assert(spec == s"zk2!hosts!/twitter/service/role/env/job:endpoint/extra")
        named += 1
        Var.value(Addr.Neg)
      }
      assert(named == 0)
      val path = Path.read("/hosts/twitter/service/role/env/job:endpoint/extra")
      assert(namer.bind(NameTree.Leaf(path)).sample() == NameTree.Neg)
      assert(named == 1)
    }
  }
}
