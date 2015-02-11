package com.twitter.finagle.client

import com.twitter.finagle.{Addr, Service, Stack, StackBuilder, ServiceFactory}
import com.twitter.finagle.client.AddrMetadataExtraction.AddrMetadata
import com.twitter.finagle.loadbalancer.LoadBalancerFactory.Dest
import com.twitter.finagle.stack.nilStack
import com.twitter.util.{Await, Future, Var}
import java.net.SocketAddress
import org.junit.runner.RunWith
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class AddrMetadataExtractionTest extends FunSuite with AssertionsForJUnit {
  class Ctx {
    val metadata = Addr.Metadata("foo" -> "bar")
    val addrBound = Addr.Bound(Set.empty[SocketAddress], metadata)

    def verifyModule(expected: Addr.Metadata) =
      new Stack.Module[ServiceFactory[String, String]] {
        val role = Stack.Role("verifyModule")
        val description = "Verify that the metadata was set properly"
        val parameters = Seq(implicitly[Stack.Param[AddrMetadata]])

        def make(params: Stack.Params, next: Stack[ServiceFactory[String, String]]) = {
          val AddrMetadata(metadata) = params[AddrMetadata]
          assert(expected === metadata)
          Stack.Leaf(this, ServiceFactory.const(Service.mk[String, String](Future.value)))
        }
      }

    def verify(addr: Var[Addr], expected: Addr.Metadata) = {
      val factory = new StackBuilder[ServiceFactory[String, String]](nilStack[String, String])
        .push(verifyModule(expected))
        .push(AddrMetadataExtraction.module)
        .make(Stack.Params.empty + Dest(addr))

      factory()
    }
  }

  test("extract from Addr.Bound")(new Ctx {
    Await.result(verify(Var(addrBound), metadata))
  })

  test("empty for Addr.Neg")(new Ctx {
    Await.result(verify(Var(Addr.Neg), Addr.Metadata.empty))
  })

  test("undefined for Addr.Pending until Addr.Bound")(new Ctx {
    val addr = Var[Addr](Addr.Pending)
    val result = verify(addr, metadata)
    assert(!result.isDefined)
    addr() = addrBound
    Await.result(result)
  })

  test("empty for Addr.Failed")(new Ctx {
    Await.result(verify(Var(Addr.Failed(new RuntimeException)), Addr.Metadata.empty))
  })
}