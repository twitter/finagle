package com.twitter.finagle.client

import com.twitter.conversions.time._
import com.twitter.finagle.Stack.{NoOpModule, Params}
import com.twitter.finagle._
import com.twitter.finagle.service._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util._
import com.twitter.util.registry.{Entry, GlobalRegistry, SimpleRegistry}
import java.util.concurrent.atomic.AtomicInteger
import org.junit.runner.RunWith
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

private object MethodBuilderTest {
  private val neverSvc: Service[Int, Int] =
    Service.mk { _ => Future.never }

  val totalTimeoutStack: Stack[ServiceFactory[Int, Int]] = {
    val svcFactory = ServiceFactory.const(neverSvc)
    // use a no-op module to verify it will get swapped out
    val totalModule = new NoOpModule[ServiceFactory[Int, Int]](
      TimeoutFilter.totalTimeoutRole, "testing total timeout")
    totalModule.toStack(Stack.Leaf(Stack.Role("test"), svcFactory))
  }

  val stack: Stack[ServiceFactory[Int, Int]] = {
    val svcFactory = ServiceFactory.const(neverSvc)
    TimeoutFilter.clientModule[Int, Int]
      .toStack(Stack.Leaf(Stack.Role("test"), svcFactory))
  }

  case class TestStackClient(
      override val stack: Stack[ServiceFactory[Int, Int]],
      override val params: Params)
    extends StackClient[Int, Int] { self =>

    def withStack(stack: Stack[ServiceFactory[Int, Int]]): StackClient[Int, Int] =
      TestStackClient(stack, self.params)

    def withParams(ps: Stack.Params): StackClient[Int, Int] =
      TestStackClient(self.stack, ps)

    def newClient(dest: Name, label: String): ServiceFactory[Int, Int] =
      stack.make(params)

    def newService(dest: Name, label: String): Service[Int, Int] =
      new FactoryToService(newClient(dest, label))
  }
}

@RunWith(classOf[JUnitRunner])
class MethodBuilderTest
  extends FunSuite
  with Matchers
  with Eventually
  with IntegrationPatience {

  import MethodBuilderTest._

  test("retries do not see the total timeout") {
    val stats = new InMemoryStatsReceiver()
    val params =
      Stack.Params.empty +
        param.Stats(stats) +
        Retries.Budget(RetryBudget.Infinite)
    val stackClient = TestStackClient(totalTimeoutStack, params)
    val methodBuilder = MethodBuilder.from("retry_it", stackClient)

    val client = methodBuilder
      .withTimeout.total(10.milliseconds)
      .withRetry.forClassifier {
        case ReqRep(_, Throw(_: GlobalRequestTimeoutException)) =>
          ResponseClass.RetryableFailure
      }
      .newService("a_client")

    intercept[GlobalRequestTimeoutException] {
      Await.result(client(1), 5.seconds)
    }
    // while we have a RetryFilter, the underlying service returns `Future.never`
    // and as such, the stats are never updated.
    assert(stats.stat("retry_it", "a_client", "retries")() == Seq.empty)
  }

  test("per-request, retries, and total timeouts") {
    val stats = new InMemoryStatsReceiver()
    val timer = new MockTimer()
    val params =
      Stack.Params.empty +
        param.Timer(timer) +
        param.Stats(stats) +
        Retries.Budget(RetryBudget.Infinite)

    val perReqTimeout = 50.milliseconds
    val totalTimeout = perReqTimeout * 2 + 20.milliseconds
    val svc: Service[Int, Int] = Service.mk { i =>
      Future.sleep(perReqTimeout + 1.millis)(timer).map(_ => i)
    }

    val stack = TimeoutFilter.clientModule[Int, Int]
      .toStack(Stack.Leaf(Stack.Role("test"), ServiceFactory.const(svc)))
    val stackClient = TestStackClient(stack, params)
    val methodBuilder = MethodBuilder.from("together", stackClient)

    // the first 2 attempts will hit the per-request timeout, with each
    // being retried. then the the 3 attempt (2nd retry) should run into
    // the total timeout.
    val client = methodBuilder
      .withTimeout.perRequest(perReqTimeout)
      .withTimeout.total(totalTimeout)
      .withRetry.forClassifier {
        case ReqRep(_, Throw(_: IndividualRequestTimeoutException)) =>
          ResponseClass.RetryableFailure
      }
      .newService("a_client")

    Time.withCurrentTimeFrozen { tc =>
      // issue the request
      val rep = client(1)
      assert(!rep.isDefined)

      // hit the 1st per-req timeout.
      tc.advance(perReqTimeout)
      timer.tick()
      assert(!rep.isDefined)

      // hit the 2nd per-req timeout.
      tc.advance(perReqTimeout)
      timer.tick()
      assert(!rep.isDefined)

      // hit the total timeout
      tc.advance(20.milliseconds)
      timer.tick()
      assert(rep.isDefined)

      intercept[GlobalRequestTimeoutException] {
        Await.result(rep, 5.seconds)
      }

      eventually {
        // confirm there were 2 retries issued
        assert(stats.stat("together", "a_client", "retries")() == Seq(2))
      }
    }
  }

  test("logical stats") {
    val stats = new InMemoryStatsReceiver()
    val timer = new MockTimer()
    val clientLabel = "the_client"
    val params =
      Stack.Params.empty +
        param.Label(clientLabel) +
        param.Timer(timer) +
        param.Stats(stats) +
        Retries.Budget(RetryBudget.Infinite)

    val perReqTimeout = 50.milliseconds
    val attempts = new AtomicInteger(0)
    val svc: Service[Int, Int] = Service.mk { i =>
      val num = attempts.incrementAndGet()
      if (num <= 1)
        Future.value(i).delayed(perReqTimeout + 1.millis)(timer)
      else
        Future.value(i)
    }

    val stack = TimeoutFilter.clientModule[Int, Int]
      .toStack(Stack.Leaf(Stack.Role("test"), ServiceFactory.const(svc)))
    val stackClient = TestStackClient(stack, params)
    val methodBuilder = MethodBuilder.from("destination", stackClient)

    // the first attempts will hit the per-request timeout and will be
    // retried. then the retry should succeed.
    val methodName = "a_method"
    val client = methodBuilder
      .withTimeout.perRequest(perReqTimeout)
      .withRetry.forClassifier {
        case ReqRep(_, Throw(_: IndividualRequestTimeoutException)) =>
          ResponseClass.RetryableFailure
      }
      .newService(methodName)

    Time.withCurrentTimeFrozen { tc =>
      // issue the request
      val rep = client(1)
      assert(!rep.isDefined)

      // hit the 1st per-req timeout.
      tc.advance(perReqTimeout)
      timer.tick()

      // then let the retry go which should immediately succeed.
      val delta = 5.milliseconds
      tc.advance(delta)
      timer.tick()
      Await.ready(rep, 5.seconds)

      assert(2 == attempts.get)
      eventually {
        // confirm there was 1 retry
        assert(stats.stat(clientLabel, methodName, "retries")() == Seq(1))

        // the logical stats should only show 1 successful request despite
        // 2 "actual" requests
        assert(stats.counter(clientLabel, methodName, "logical", "requests")() == 1)
        assert(stats.counter(clientLabel, methodName, "logical", "success")() == 1)
        val latencies = stats.stat(clientLabel, methodName, "logical", "request_latency_ms")()
        assert(latencies.size == 1)
        assert(latencies.head <= (perReqTimeout + delta).inMillis)
      }

      val otherMethod = "other"
      val client2 = methodBuilder.newService(otherMethod)

      // issue the request
      val rep2 = client2(2)
      Await.ready(rep2, 5.seconds)

      assert(3 == attempts.get)
      eventually {
        // the logical stats should be separate per-"method"
        assert(stats.counter(clientLabel, otherMethod, "logical", "requests")() == 1)
      }
    }
  }

  test("newService's are added to the Registry") {
    val registry = new SimpleRegistry()
    GlobalRegistry.withRegistry(registry) {
      val protocolLib = "test_lib"
      val clientName = "some_svc"
      val addr = "test_addr"
      val stats = new InMemoryStatsReceiver()
      val params =
        Stack.Params.empty +
          param.Stats(stats) +
          param.Label(clientName) +
          param.ProtocolLibrary(protocolLib)
      val stackClient = TestStackClient(stack, params)
      val methodBuilder = MethodBuilder.from(addr, stackClient)

      def key(name: String, suffix: String*): Seq[String] =
        Seq("client", protocolLib, clientName, addr, "methods", name) ++ suffix

      def filteredRegistry: Set[Entry] =
        registry.filter { entry =>
          entry.key.head == "client"
        }.toSet

      // test a "vanilla" one
      val vanillaSvc = methodBuilder.newService("vanilla")
      val vanillaEntries = Set(
        Entry(key("vanilla", "statsReceiver"), s"InMemoryStatsReceiver/$clientName/vanilla"),
        Entry(key("vanilla", "retry"), "Config(DefaultResponseClassifier)")
      )
      assert(filteredRegistry == vanillaEntries)

      // test with retries disabled and timeouts
      val sundaeSvc = methodBuilder
        .withTimeout.total(10.seconds)
        .withTimeout.perRequest(1.second)
        .withRetry.disabled
        .newService("sundae")
      val sundaeEntries = Set(
        Entry(key("sundae", "statsReceiver"), s"InMemoryStatsReceiver/$clientName/sundae"),
        Entry(key("sundae", "retry"), "Config(Disabled)"),
        Entry(key("sundae", "timeout", "total"), "10.seconds"),
        Entry(key("sundae", "timeout", "per_request"), "1.seconds")
      )
      filteredRegistry should contain theSameElementsAs (vanillaEntries ++ sundaeEntries)

      val vanillaClose = vanillaSvc.close()
      assert(!vanillaClose.isDefined)
      filteredRegistry should contain theSameElementsAs sundaeEntries

      Await.ready(sundaeSvc.close(), 5.seconds)
      assert(vanillaClose.isDefined)
      assert(Set.empty == filteredRegistry)
    }
  }

  test("stats are filtered") {
    val stats = new InMemoryStatsReceiver()
    val clientLabel = "the_client"
    val params =
      Stack.Params.empty +
        param.Label(clientLabel) +
        param.Stats(stats)

    val failure = Failure("some reason", new RuntimeException("welp"))
      .withSource(Failure.Source.Service, "test_service")
    val svc: Service[Int, Int] = new FailedService(failure)

    val stack = Stack.Leaf(Stack.Role("test"), ServiceFactory.const(svc))
    val stackClient = TestStackClient(stack, params)

    val methodBuilder = MethodBuilder.from("destination", stackClient)

    // the first attempts will hit the per-request timeout and will be
    // retried. then the retry should succeed.
    val methodName = "a_method"
    val client = methodBuilder.newService(methodName)

    // issue a failing request
    intercept[Failure] {
      Await.result(client(1), 5.seconds)
    }

    // verify the metrics are getting filtered down
    assert(!stats.gauges.contains(Seq(clientLabel, methodName, "logical", "pending")))

    val failureCounters = stats.counters.exists { case (names, _) =>
      names.containsSlice(Seq(clientLabel, methodName, "logical", "failures")) ||
        names.containsSlice(Seq(clientLabel, methodName, "logical", "sourcedfailures"))
    }
    assert(!failureCounters)
  }

  test("underlying service is reference counted") {
    val svc: Service[Int, Int] = new Service[Int, Int] {
      private[this] var closed = false
      def apply(req: Int): Future[Int] = ???
      override def close(deadline: Time): Future[Unit] = {
        closed = true
        Future.Done
      }
      override def status: Status =
        if (closed) Status.Closed else Status.Open
    }
    val svcFac: ServiceFactory[Int, Int] = new ServiceFactory[Int, Int] {
      def apply(conn: ClientConnection): Future[Service[Int, Int]] =
        Future.value(svc)
      def close(deadline: Time): Future[Unit] =
        svc.close(deadline)
    }
    val stk = Stack.Leaf(Stack.Role("test"), svcFac)
    val stackClient = TestStackClient(stk, Stack.Params.empty)
    val methodBuilder = MethodBuilder.from("refcounts", stackClient)

    val m1 = methodBuilder.newService("method1")
    val m2 = methodBuilder.newService("method2")
    assert(m1.isAvailable)
    assert(m2.isAvailable)
    assert(svc.isAvailable)

    // closing 1 method should not touch the other
    val m1Close = m1.close()
    assert(!m1Close.isDefined)
    assert(!m1.isAvailable)
    intercept[ServiceClosedException] { Await.result(m1(1), 5.seconds) }
    assert(m2.isAvailable)
    assert(svc.isAvailable)

    // validate that a second close of the same method does nothing
    assert(!m1Close.isDefined)
    assert(!m1.isAvailable)
    assert(m2.isAvailable)
    assert(svc.isAvailable)

    // validate that closing the last method closes the underlying service.
    Await.ready(m2.close(), 5.seconds)
    assert(m1Close.isDefined)
    assert(!m1.isAvailable)
    assert(!m2.isAvailable)
    intercept[ServiceClosedException] { Await.result(m2(1), 5.seconds) }
    assert(!svc.isAvailable)
  }

  test("failures are annotated with the method name") {
    val params = Stack.Params.empty

    val failure = Failure("some reason", new RuntimeException("welp"))
    val svc: Service[Int, Int] = new FailedService(failure)

    val stack = Stack.Leaf(Stack.Role("test"), ServiceFactory.const(svc))
    val stackClient = TestStackClient(stack, params)

    val methodBuilder = MethodBuilder.from("destination", stackClient)

    val methodName = "a_method"
    val client = methodBuilder.newService(methodName)

    // issue a failing request
    val f = intercept[Failure] {
      Await.result(client(1), 5.seconds)
    }

    assert(f.getSource(Failure.Source.Method).contains(methodName))
  }

}
