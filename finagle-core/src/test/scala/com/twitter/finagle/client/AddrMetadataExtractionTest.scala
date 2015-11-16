package com.twitter.finagle.client

import com.twitter.finagle.{Addr, Name, Path, Service, Stack, StackBuilder, ServiceFactory}
import com.twitter.finagle.client.AddrMetadataExtraction.AddrMetadata
import com.twitter.finagle.factory.BindingFactory
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.stack.nilStack
import com.twitter.util.{Await, Future, Var}
import java.net.SocketAddress
import org.junit.runner.RunWith
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class AddrMetadataExtractionTest extends FunSuite with AssertionsForJUnit {
  class Ctx {
    val unbound = Name.Path(Path.read("/$/fail"))
    val metadata = Addr.Metadata("foo" -> "bar")
    val addrBound = Addr.Bound(Set.empty[SocketAddress], metadata)
    val vaddrBound = Var(addrBound)
    val bound = Name.Bound(vaddrBound, Path.read("/baz"))

    def verifyModule(expected: Addr.Metadata) =
      new Stack.Module1[AddrMetadata, ServiceFactory[String, String]] {
        val role = Stack.Role("verifyModule")
        val description = "Verify that the metadata was set properly"

        def make(param: AddrMetadata, next: ServiceFactory[String, String]) = {
          assert(expected == param.metadata)
          ServiceFactory.const(Service.mk[String, String](Future.value))
        }
      }

    def verify(addr: Var[Addr], name: Name, expected: Addr.Metadata) = {
      val factory = new StackBuilder[ServiceFactory[String, String]](nilStack[String, String])
        .push(verifyModule(expected))
        .push(AddrMetadataExtraction.module)
        .make(Stack.Params.empty + LoadBalancerFactory.Dest(addr) + BindingFactory.Dest(name))

      factory()
    }
  }

  test("extract from Addr.Bound")(new Ctx {
    Await.result(verify(Var(addrBound), unbound, metadata))
  })

  test("add bound name id")(new Ctx {
    val vaddr = Var(addrBound)
    val name = Name.Bound(vaddr, "baz")
    Await.result(verify(vaddr, name, metadata ++ Addr.Metadata("id" -> "baz")))
  })

  test("add bound name path id")(new Ctx {
    Await.result(verify(vaddrBound, bound, metadata ++ Addr.Metadata("id" -> "/baz")))
  })

  test("empty for Addr.Neg")(new Ctx {
    Await.result(verify(Var(Addr.Neg), unbound, Addr.Metadata.empty))
  })

  test("undefined for Addr.Pending until Addr.Bound")(new Ctx {
    val addr = Var[Addr](Addr.Pending)
    val result = verify(addr, unbound, metadata)
    assert(!result.isDefined)
    addr() = addrBound
    Await.result(result)
  })

  test("just id for Addr.Failed")(new Ctx {
    Await.result(
      verify(Var(Addr.Failed(new RuntimeException)), bound, Addr.Metadata("id" -> "/baz")))
  })
}
