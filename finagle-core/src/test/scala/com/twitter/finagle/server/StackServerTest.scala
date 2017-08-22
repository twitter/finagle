package com.twitter.finagle.server

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.context.{Contexts, Deadline}
import com.twitter.finagle.filter.ServerAdmissionControl
import com.twitter.finagle.param.{Stats, Timer}
import com.twitter.finagle.service.{ExpiringService, TimeoutFilter}
import com.twitter.finagle.stack.Endpoint
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.util.StackRegistry
import com.twitter.util.{Await, Duration, Future, MockTimer, Promise, Time}
import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import org.scalatest.FunSuite

class StackServerTest extends FunSuite {
  test("Deadline isn't changed until after it's recorded") {
    val echo = ServiceFactory.const(Service.mk[Unit, Deadline] { unit =>
      Future.value(Contexts.broadcast(Deadline))
    })
    val stack = StackServer.newStack[Unit, Deadline] ++ Stack.Leaf(Endpoint, echo)
    val statsReceiver = new InMemoryStatsReceiver
    val factory =
      stack.make(StackServer.defaultParams + TimeoutFilter.Param(1.second) + Stats(statsReceiver))
    val svc = Await.result(factory(), 5.seconds)
    Time.withCurrentTimeFrozen { ctl =>
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(5.seconds)) {
        ctl.advance(1.second)
        val result = svc(())

        // we should be one second ahead
        assert(statsReceiver.stats(Seq("transit_latency_ms"))(0) == 1.second.inMilliseconds.toFloat)

        // but the deadline inside the service's closure should be updated
        assert(Await.result(result) == Deadline.ofTimeout(1.second))
      }
    }
  }

  test("StackServer uses ExpiringService") {
    @volatile var closed = false
    val connSF = new ServiceFactory[Int, Int] {
      val svc = Service.mk[Int, Int] { i =>
        Future.value(i)
      }
      def apply(conn: ClientConnection) = {
        conn.onClose.ensure { closed = true }
        Future.value(svc)
      }
      def close(deadline: Time) = Future.Done
    }
    val stack = StackServer.newStack[Int, Int] ++ Stack.Leaf(Endpoint, connSF)
    val sr = new InMemoryStatsReceiver
    val timer = new MockTimer
    val lifeTime = 1.second
    val factory = stack.make(
      StackServer.defaultParams +
        ExpiringService.Param(idleTime = Duration.Top, lifeTime = lifeTime) +
        Timer(timer) +
        Stats(sr)
    )

    val conn = new ClientConnection {
      val closed = new Promise[Unit]
      def remoteAddress: SocketAddress = new SocketAddress {}
      def localAddress: SocketAddress = new SocketAddress {}
      def close(deadline: Time): Future[Unit] = {
        closed.setDone()
        Future.Done
      }
      def onClose: Future[Unit] = closed
    }

    val svc = Await.result(factory(conn), 5.seconds)

    Time.withCurrentTimeFrozen { ctl =>
      assert(Await.result(svc(1), 5.seconds) == 1)
      ctl.advance(lifeTime * 2)
      timer.tick()
      assert(closed)
    }
  }

  test("StackServer added to server registry") {
    ServerRegistry.clear()
    val name = "testServer"
    val s = Service.const[String](Future.value("foo"))
    val server = StringServer.server.withLabel(name).serve(new InetSocketAddress(0), s)

    // assert registry entry added
    assert(ServerRegistry.registrants.count { e: StackRegistry.Entry =>
      val param.Label(actual) = e.params[param.Label]
      name == actual
    } == 1)

    Await.ready(server.close(), 10.seconds)

    // assert registry entry removed
    assert(ServerRegistry.registrants.count { e: StackRegistry.Entry =>
      val param.Label(actual) = e.params[param.Label]
      name == actual
    } == 0)
  }

  test("ListeningStackServer closes ServiceFactories") {
    val serviceFactoryClosed: Promise[Unit] = new Promise[Unit]
    val fn: ServiceFactory[String, String] => ServiceFactory[String, String] = { factory =>
      new ServiceFactoryProxy[String, String](factory) {
        override def close(deadline: Time): Future[Unit] = {
          serviceFactoryClosed.setDone()
          factory.close(deadline)
        }
      }
    }

    val csf = CanStackFrom.fromFun[ServiceFactory[String, String]]
    val stackable = csf.toStackable(new Stack.Role("something"), fn)
    val stk: Stack[ServiceFactory[String, String]] = StackServer.newStack.prepend(stackable)
    val factory = ServiceFactory.const(Service.const[String](Future.value("hi")))

    val server = StringServer.server
      .withStack(stk)
      .serve(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), factory)
    Await.result(server.close(), 2.seconds)
    assert(serviceFactoryClosed.isDefined)
  }

  test("ensure onServerClosed Promise is satisfied upon server close") {
    val wasPromiseSatisfied = new Promise[Unit]

    class ClosingFilter[Req, Rep](onServerClose: Future[Unit]) extends SimpleFilter[Req, Rep] {
      def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
        service(req)
      }
      onServerClose.onSuccess { _ =>
        wasPromiseSatisfied.setDone()
      }
    }

    object ClosingFilter2 {
      val name = "closing filter"

      val mkFilter = { params: ServerAdmissionControl.ServerParams =>
        new Filter.TypeAgnostic {
          def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] =
            new ClosingFilter[Req, Rep](onServerClose = params.onServerClose)
        }
      }
    }
    try {
      ServerAdmissionControl.register(ClosingFilter2.name, ClosingFilter2.mkFilter)
      val echo = ServiceFactory.const(Service.mk[String, String](s => Future.value(s)))
      val stack = StackServer.newStack[String, String] ++ Stack.Leaf(Endpoint, echo)
      val factory = ServiceFactory.const(Service.const[String](Future.value("hi")))

      val server = StringServer.server
        .withStack(stack)
        .serve(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), factory)

      Await.ready(server.close(), 5.seconds)
      assert(wasPromiseSatisfied.isDefined)
    } finally {
      ServerAdmissionControl.unregister(ClosingFilter2.name)
    }
  }
}
