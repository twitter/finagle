package com.twitter.finagle.server

import com.twitter.concurrent.AsyncSemaphore
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Stack.Module0
import com.twitter.finagle._
import com.twitter.finagle.context.{Contexts, Deadline}
import com.twitter.finagle.filter.RequestSemaphoreFilter
import com.twitter.finagle.param.{Stats, Timer}
import com.twitter.finagle.server.utils.StringServer
import com.twitter.finagle.service.MetricBuilderRegistry.ExpressionNames.{
  deadlineRejectName,
  latencyName,
  successRateName,
  throughputName
}
import com.twitter.finagle.service.{ExpiringService, TimeoutFilter}
import com.twitter.finagle.ssl.session.{NullSslSessionInfo, SslSessionInfo}
import com.twitter.finagle.stack.Endpoint
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.exp.ExpressionSchemaKey
import com.twitter.finagle.util.StackRegistry
import com.twitter.util.{Await, Duration, Future, MockTimer, Promise, Time}
import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import org.scalatest.concurrent.Eventually
import org.scalatest.funsuite.AnyFunSuite

class StackServerTest extends AnyFunSuite with Eventually {

  test("withStack (Function1)") {
    val module = new Module0[ServiceFactory[String, String]] {
      def make(next: ServiceFactory[String, String]): ServiceFactory[String, String] = ???
      def role: Stack.Role = Stack.Role("no-op")
      def description: String = "no-op"
    }

    val init = StringServer.server.stack
    assert(!init.contains(module.role))

    val modified = StringServer.server.withStack(_.prepend(module)).stack
    assert(modified.contains(module.role))

    init.tails.map(_.head).foreach { stackHead => assert(modified.contains(stackHead.role)) }
  }

  test("Deadline isn't changed until after it's recorded") {
    val echo = ServiceFactory.const(Service.mk[Unit, Deadline] { unit =>
      Future.value(Contexts.broadcast(Deadline))
    })
    val stack = StackServer.newStack[Unit, Deadline] ++ Stack.leaf(Endpoint, echo)
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
      val svc = Service.mk[Int, Int] { i => Future.value(i) }
      def apply(conn: ClientConnection) = {
        conn.onClose.ensure { closed = true }
        Future.value(svc)
      }
      def close(deadline: Time) = Future.Done
    }
    val stack = StackServer.newStack[Int, Int] ++ Stack.leaf(Endpoint, connSF)
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
      def sslSessionInfo: SslSessionInfo = NullSslSessionInfo
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
    val stackable = csf.toStackable(Stack.Role("something"), fn)
    val stk: Stack[ServiceFactory[String, String]] = StackServer.newStack.prepend(stackable)
    val factory = ServiceFactory.const(Service.const[String](Future.value("hi")))

    val server = StringServer.server
      .withStack(stk)
      .serve(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), factory)
    Await.result(server.close(), 2.seconds)
    assert(serviceFactoryClosed.isDefined)
  }

  test("Rejections from RequestSemaphoreFilter are captured in stats") {
    val neverRespond = ServiceFactory.const(Service.mk[String, String](_ => Future.never))
    val stack = StackServer.newStack[String, String] ++ Stack.leaf(Endpoint, neverRespond)
    val sr = new InMemoryStatsReceiver
    val factory = stack.make(
      StackServer.defaultParams +
        RequestSemaphoreFilter.Param(Some(new AsyncSemaphore(initialPermits = 1, maxWaiters = 0))) +
        Stats(sr)
    )
    val svc = Await.result(factory(), 5.seconds)

    // first request should hang
    svc("foo")

    // second request should be rejected by the filter
    val exc = intercept[Failure] {
      Await.result(svc("foo"), 5.seconds)
    }

    assert(exc.isFlagged(FailureFlags.Rejected) && exc.isFlagged(FailureFlags.Retryable))

    eventually {
      // First request never returns; dispatches is only incremented for the second request.
      assert(sr.counters(Seq("requests")) == 1)
      assert(sr.counters(Seq("failures", "rejected")) == 1)
      assert(sr.counters(Seq("failures", "restartable")) == 1)
    }
  }

  test("Items appended to DefaultTransformer appear in the listing") {
    val transformer = new StackTransformer {
      val name = "id"
      def apply[A, B](s: Stack[ServiceFactory[A, B]]) = s
    }
    assert(!StackServer.DefaultTransformer.transformers.contains(transformer))

    val len = StackServer.DefaultTransformer.transformers.length
    StackServer.DefaultTransformer.append(transformer)
    assert(StackServer.DefaultTransformer.transformers.contains(transformer))
    assert(StackServer.DefaultTransformer.transformers.length == len + 1)
  }

  test("serve() uses DefaultTransformer") {
    implicit val stringParam = Stack.Param("")

    var didRun = false
    val testRole = Stack.Role("test")
    def testModule[A, B]: Stackable[ServiceFactory[A, B]] =
      new Stack.Module1[String, ServiceFactory[A, B]] {
        val role = testRole
        val description = role.toString
        def make(greeting: String, next: ServiceFactory[A, B]) = {
          // We test param and module transformations differently. The note
          // below explains why.
          assert(greeting == "hello")
          didRun = true
          next
        }
      }

    def hello[A, B]: Stackable[ServiceFactory[A, B]] =
      new Stack.Module[ServiceFactory[A, B]] {
        val role = Stack.Role("hello")
        val description = role.toString
        val parameters = Seq(implicitly[Stack.Param[String]])
        def make(params: Stack.Params, next: Stack[ServiceFactory[A, B]]) = {
          Stack.leaf(this, next.make(params + "hello"))
        }
      }

    StackServer.DefaultTransformer.append(
      new StackTransformer {
        val name = "test"
        def apply[A, B](stack: Stack[ServiceFactory[A, B]]) =
          stack
          // testModule contains the assertion for the "hello" param.
            .prepend(testModule)
            .prepend(hello)
      }
    )

    ServerRegistry.clear()
    val svc = Service.const(Future.value("ok"))
    val server = StringServer.server.serve(new InetSocketAddress(0), svc)
    val Seq(entry) = ServerRegistry.registrants.toSeq
    val stack = entry.stack.asInstanceOf[Stack[ServiceFactory[String, String]]]

    // Note: we can consult the stack directly for the existence of modules.
    // By inspecting the stack for the test role, we can be certain that the
    // DefaultTransformer was able to add it. Params can't be observed from the
    // Stack in the same way: they are hidden from query via the Stack API. We
    // must test for expected params via an assert in the module added above.
    assert(stack.contains(testRole))
    assert(didRun)

    Await.ready(server.close(), 10.seconds)
  }

  private[this] def nameToKey(name: String): ExpressionSchemaKey =
    ExpressionSchemaKey(name, Map(), Seq())

  test("StackServer has MetricBuilderRegistry configured instruments default expressions") {
    val sf = ServiceFactory.const(Service.mk[String, String](_ => Future.value("hi")))
    val stack = StackServer.newStack[String, String] ++ Stack.leaf(Endpoint, sf)

    val sr = new InMemoryStatsReceiver
    val svc = Await.result(stack.make(StackServer.defaultParams + Stats(sr))(), 5.seconds)
    Await.result(svc("foo"), 5.seconds)

    // ACRejectedCounter is not configured in the default stack
    // We won't create acRejectName which uses that metric
    assert(sr.expressions.contains(nameToKey(successRateName)))
    assert(sr.expressions.contains(nameToKey(throughputName)))
    assert(sr.expressions.contains(nameToKey(latencyName)))
    assert(sr.expressions.contains(nameToKey(deadlineRejectName)))
  }
}
