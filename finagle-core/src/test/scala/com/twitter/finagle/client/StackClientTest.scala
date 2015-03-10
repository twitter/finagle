package com.twitter.finagle.client

import com.twitter.finagle.Stack.Module0
import com.twitter.finagle._
import com.twitter.finagle.factory.BindingFactory
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.service.FailFastFactory.FailFast
import com.twitter.finagle.service.RequeueingFilter.MaxTries
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

  test("Client added to client registry")(new Ctx {
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

  trait RequeueCtx {
    var count = 0
    var _status: Status = Status.Open

    var runSideEffect = (_: Int) => false
    var sideEffect = () => ()

    val stubLB = new ServiceFactory[String, String] {
      def apply(conn: ClientConnection) = Future.value(new Service[String, String] {
        def apply(request: String): Future[String] = {
          count += 1
          if (runSideEffect(count)) sideEffect()
          Future.exception(WriteException(new Exception("boom")))
        }

        override def close(deadline: Time) = Future.Done
      })

      def close(deadline: Time) = Future.Done

      override def status = _status
    }

    val sr = new InMemoryStatsReceiver
    val client = stringClient.configured(param.Stats(sr))

    val stk = client.stack.replace(
      LoadBalancerFactory.role,
      (_: ServiceFactory[String, String]) => stubLB
    )

    val cl = client
      .withStack(stk)
      .newClient("myclient=/$/inet/localhost/0")

    def automaticRetries = sr.stats(Seq("myclient", "automatic", "retries"))
  }

  test("requeue failing requests when the stack is Open")(new RequeueCtx {
      // failing request and Open load balancer => max requeues
      Await.ready(cl().map(_("hi")))

      // expect one less retry than requeue try limit
      assert(automaticRetries === Seq(MaxTries - 1))
  })

  for (status <- Seq(Status.Busy, Status.Closed)) {
    test(s"don't requeue failing requests when the stack is $status")(new RequeueCtx {
      // failing request and Busy | Closed load balancer => zero requeues
      _status = status
      Await.ready(cl().map(_("hi")))
      assert(automaticRetries === Seq(0))
    })
  }

  test("dynamically stop requeuing")( new RequeueCtx {
      // load balancer begins Open, becomes Busy after 10 requeues => 10 requeues
      _status = Status.Open
      runSideEffect = _ > 10
      sideEffect = () => _status = Status.Busy
      Await.ready(cl().map(_("hi")))
      assert(automaticRetries === Seq(10))
  })

  test("Requeues all go to the same cluster in a Union") {
    /*
     * Once we have distributed a request to a particular cluster (in
     * BindingFactory), retries should go to the same cluster rather
     * than being redistributed (possibly to a different cluster).
     */

    class CountFactory extends ServiceFactory[Unit, Unit] {
      var count = 0

      val service = new Service[Unit, Unit] {
        def apply(request: Unit): Future[Unit] = {
          count = count + 1
          Future.exception(WriteException(null))
        }
      }

      def apply(conn: ClientConnection) = Future.value(service)
      def close(deadline: Time) = Future.Done
    }

    val fac1 = new CountFactory
    val fac2 = new CountFactory

    val addr1 = new InetSocketAddress(1729)
    val addr2 = new InetSocketAddress(1730)

    // override name resolution to a Union of two addresses
    val dtab = new Dtab(Dtab.base) {
      override def lookup(path: Path): Activity[NameTree[Name]] =
        Activity.value(NameTree.Union(
          NameTree.Weighted(1D, NameTree.Leaf(Name.bound(addr1))),
          NameTree.Weighted(1D, NameTree.Leaf(Name.bound(addr2)))))
    }

    val stack = StackClient.newStack[Unit, Unit]

      // direct the two addresses to the two service factories instead
      // of trying to connect to them
      .replace(LoadBalancerFactory.role,
        new Stack.Module1[LoadBalancerFactory.Dest, ServiceFactory[Unit, Unit]] {
          val role = new Stack.Role("role")
          val description = "description"
          def make(dest: LoadBalancerFactory.Dest, next: ServiceFactory[Unit, Unit]) = {
            val LoadBalancerFactory.Dest(va) = dest
            va.sample match {
              case Addr.Bound(addrs, _) if addrs == Set(addr1) => fac1
              case Addr.Bound(addrs, _) if addrs == Set(addr2) => fac2
              case _ => throw new IllegalArgumentException("wat")
            }
          }
        })

    val service =
      new FactoryToService(stack.make(Stack.Params.empty +
        FactoryToService.Enabled(true) +
        BindingFactory.BaseDtab(() => dtab)))

    intercept[ChannelWriteException] {
      Await.result(service(()))
    }

    // all retries go to one service
    assert(
      (fac1.count == MaxTries && fac2.count == 0) ||
        (fac2.count == MaxTries && fac1.count == 0))
  }

  test("StackBasedClient.configured is a StackClient") {
    // compilation test
    val client: StackBasedClient[String, String] = stringClient
    val client2: StackBasedClient[String, String] =
      client.configured(param.Label("foo"))
    val client3: StackBasedClient[String, String] =
      client.configured[param.Label]((param.Label("foo"), param.Label.param))
  }

  test("StackClient.configured is a StackClient") {
    // compilation test
    val client: StackClient[String, String] = stringClient
    val client2: StackClient[String, String] =
      client.configured(param.Label("foo"))
    val client3: StackClient[String, String] =
      client.configured[param.Label]((param.Label("foo"), param.Label.param))
  }
}
