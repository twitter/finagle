package com.twitter.finagle.client

import com.twitter.finagle.Stack.Module0
import com.twitter.finagle._
import com.twitter.finagle.factory.BindingFactory
import com.twitter.finagle.service.FailFastFactory.FailFast
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.util.StackRegistry
import com.twitter.finagle.{param, Name}
import com.twitter.util._
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.FunSuite
import org.scalatest.junit.{AssertionsForJUnit, JUnitRunner}

@RunWith(classOf[JUnitRunner])
class StackClientTest extends FunSuite
  with StringClient
  with AssertionsForJUnit
  with Eventually
  with IntegrationPatience {

  trait Ctx {
    val sr = new InMemoryStatsReceiver
    val client = stringClient
      .configured(param.Stats(sr))
  }

  test("client stats are scoped to label")(new Ctx {
    // use dest when no label is set
    client.newService("inet!localhost:8080")
    eventually {
      assert(sr.counters(Seq("inet!localhost:8080", "loadbalancer", "adds")) === 1)
    }

    // use param.Label when set
    client.configured(param.Label("myclient")).newService("localhost:8080")
    eventually {
      assert(sr.counters(Seq("myclient", "loadbalancer", "adds")) === 1)
    }

    // use evaled label when both are set
    client.configured(param.Label("myclient")).newService("othername=localhost:8080")
    eventually {
      assert(sr.counters(Seq("othername", "loadbalancer", "adds")) === 1)
    }
  })

  test("Client added to client registry") (new Ctx {
    ClientRegistry.clear()

    val name = "testClient"
    client.newClient(Name.bound(new InetSocketAddress(8080)), name)
    client.newClient(Name.bound(new InetSocketAddress(8080)), name)

    assert(ClientRegistry.registrants.count {
      e: StackRegistry.Entry => name == e.name
    } === 1)
  })

  test("FailFast is respected") {
    val ctx = new Ctx { }

    val ex = new RuntimeException("lol")
    val alwaysFail = new Module0[ServiceFactory[String, String]] {
      val role = Stack.Role("lol")
      val description = "lool"
      def make(next: ServiceFactory[String, String]) =
        ServiceFactory.apply(() => Future.exception(ex))
    }

    val alwaysFailStack = new StackBuilder(stack.nilStack[String, String])
      .push(alwaysFail)
      .result
    val stk = ctx.client.stack.concat(alwaysFailStack)

    def newClient(name: String, failFastOn: Option[Boolean]): Service[String, String] = {
      var stack = ctx.client
        .configured(param.Label(name))
        .withStack(stk)
      failFastOn.foreach { ffOn =>
        stack = stack.configured(FailFast(ffOn))
      }
      val client = stack.newClient("/$/inet/localhost/0")
      new FactoryToService[String, String](client)
    }

    def testClient(name: String, failFastOn: Option[Boolean]): Unit = {
      val svc = newClient(name, failFastOn)
      val e = intercept[RuntimeException] { Await.result(svc("hi")) }
      assert(e === ex)
      failFastOn match {
        case Some(on) if !on =>
          assert(ctx.sr.counters.get(Seq(name, "failfast", "marked_dead")) === None)
          intercept[RuntimeException] { Await.result(svc("hi2")) }
        case _ =>
          eventually {
            assert(ctx.sr.counters(Seq(name, "failfast", "marked_dead")) === 1)
          }
          intercept[FailedFastException] { Await.result(svc("hi2")) }
      }
    }

    testClient("ff-client-default", None)
    testClient("ff-client-enabled", Some(true))
    testClient("ff-client-disabled", Some(false))
  }

  test("FactoryToService close propagated to underlying service") {
    /*
     * This test ensures that the following one doesn't succeed vacuously.
     */

    var closed = false

    val underlyingFactory = new ServiceFactory[Unit, Unit] {
      def apply(conn: ClientConnection) = Future.value(new Service[Unit, Unit] {
        def apply(request: Unit): Future[Unit] = Future.Unit
        override def close(deadline: Time) = {
          closed = true
          Future.Done
        }
      })
      def close(deadline: Time) = Future.Done
    }

    val stack = StackClient.newStack[Unit, Unit]
      .concat(Stack.Leaf(Stack.Role("role"), underlyingFactory))
      // don't pool or else we don't see underlying close until service is ejected from pool
      .remove(DefaultPool.Role)

    val factory = stack.make(Stack.Params.empty +
      FactoryToService.Enabled(true) +

      // default Dest is /$/fail
      BindingFactory.Dest(Name.Path(Path.read("/$/inet/localhost/0"))))

    val service = new FactoryToService(factory)
    Await.result(service(()))

    assert(closed)
  }

  test("prepFactory above FactoryToService") {
    /*
     * This approximates code in finagle-http which wraps services (in
     * prepFactory) so the close is delayed until the chunked response
     * has been read. We need prepFactory above FactoryToService or
     * else FactoryToService closes the underlying service too soon.
     */

    var closed = false

    val underlyingFactory = new ServiceFactory[Unit, Unit] {
      def apply(conn: ClientConnection) = Future.value(new Service[Unit, Unit] {
        def apply(request: Unit): Future[Unit] = Future.Unit
        override def close(deadline: Time) = {
          closed = true
          Future.Done
        }
      })
      def close(deadline: Time) = Future.Done
    }

    val stack = StackClient.newStack[Unit, Unit]
      .concat(Stack.Leaf(Stack.Role("role"), underlyingFactory))
      // don't pool or else we don't see underlying close until service is ejected from pool
      .remove(DefaultPool.Role)

      .replace(StackClient.Role.prepFactory, { next: ServiceFactory[Unit, Unit] =>
        next map { service: Service[Unit, Unit] =>
          new ServiceProxy[Unit, Unit](service) {
            override def close(deadline: Time) = Future.never
          }
        }
      })

    val factory = stack.make(Stack.Params.empty +
      FactoryToService.Enabled(true) +

      // default Dest is /$/fail
      BindingFactory.Dest(Name.Path(Path.read("/$/inet/localhost/0"))))

    val service = new FactoryToService(factory)
    Await.result(service(()))

    assert(!closed)
  }
}
