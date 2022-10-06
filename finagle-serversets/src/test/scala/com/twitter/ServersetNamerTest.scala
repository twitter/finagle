package com.twitter

import com.twitter.finagle._
import com.twitter.finagle.serverset2.Zk2Resolver
import com.twitter.util.Activity
import com.twitter.util.Var
import org.mockito.ArgumentMatchers.anyString
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.mockito.Mockito.never
import org.mockito.Mockito.times
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ServersetNamerTest extends AnyFunSuite with MockitoSugar {
  trait Ctx {
    val zk2 = mock[Zk2Resolver]
    val namer = new com.twitter.serverset(zk2)
  }

  test("binds serverset")(new Ctx {
    val addr = Addr.Bound(Address(7127))
    when(zk2.addrOf("hosts", "/twitter/service/role/env/job", Some("endpoint"), None))
      .thenReturn(Var.value(addr))

    verify(zk2, never()).addrOf(anyString, anyString, any[Option[String]], any[Option[Int]])

    val path = Path.read("/hosts/twitter/service/role/env/job:endpoint")
    namer.bind(NameTree.Leaf(path)).sample() match {
      case NameTree.Leaf(bound: Name.Bound) =>
        assert(bound.addr.sample() == addr)
        assert(bound.path == Path.empty)
        assert(
          bound.id == Path.Utf8(
            "$",
            "com.twitter.serverset",
            "hosts",
            "twitter",
            "service",
            "role",
            "env",
            "job:endpoint"
          )
        )

      case _ => fail(s"invalid name: ${path.show}")
    }
    verify(zk2, times(1)).addrOf(anyString, anyString, any[Option[String]], any[Option[Int]])
  })

  test("negative resolution")(new Ctx {
    when(zk2.addrOf("hosts", "/twitter/service/role/env/job:endpoint/extra", None, None))
      .thenReturn(Var.value(Addr.Neg))

    verify(zk2, never()).addrOf(anyString, anyString, any[Option[String]], any[Option[Int]])

    val path = Path.read("/hosts/twitter/service/role/env/job:endpoint/extra")
    assert(namer.bind(NameTree.Leaf(path)).sample() == NameTree.Neg)

    verify(zk2, times(1)).addrOf(anyString, anyString, any[Option[String]], any[Option[Int]])
  })

  test("invalid job syntax")(new Ctx {
    val path = Path.read("/hosts/twitter/service/role/env/job#")
    val act = namer.bind(NameTree.Leaf(path))
    val Activity.Failed(e: IllegalArgumentException) = act.run.sample()
    assert(e.getMessage == "invalid job syntax: job#")
    verify(zk2, never()).addrOf(anyString, anyString, any[Option[String]], any[Option[Int]])
  })
}
