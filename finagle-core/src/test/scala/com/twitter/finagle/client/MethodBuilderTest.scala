package com.twitter.finagle.client

import com.twitter.conversions.DurationOps._
import com.twitter.conversions.PercentOps._
import com.twitter.finagle.Stack.NoOpModule
import com.twitter.finagle.Stack.Params
import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.context.Deadline
import com.twitter.finagle.service._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util._
import com.twitter.util.registry.Entry
import com.twitter.util.registry.GlobalRegistry
import com.twitter.util.registry.SimpleRegistry
import com.twitter.util.tunable.Tunable
import java.util.concurrent.atomic.AtomicInteger
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.IntegrationPatience
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

private object MethodBuilderTest {
  private val neverSvc: Service[Int, Int] =
    Service.mk { _ => Future.never }

  val totalTimeoutStack: Stack[ServiceFactory[Int, Int]] = {
    val svcFactory = ServiceFactory.const(neverSvc)
    // use a no-op module to verify it will get swapped out
    val totalModule = new NoOpModule[ServiceFactory[Int, Int]](
      TimeoutFilter.totalTimeoutRole,
      "testing total timeout"
    )
    totalModule.toStack(Stack.leaf(Stack.Role("test"), svcFactory))
  }

  val stack: Stack[ServiceFactory[Int, Int]] = {
    val svcFactory = ServiceFactory.const(neverSvc)
    TimeoutFilter
      .clientModule[Int, Int]
      .toStack(Stack.leaf(Stack.Role("test"), svcFactory))
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

class MethodBuilderTest
    extends AnyFunSuite
    with Matchers
    with Eventually
    with MockitoSugar
    with IntegrationPatience {

  import MethodBuilderTest._

  def awaitResult[T](a: Awaitable[T], d: Duration = 5.seconds): T =
    Await.result(a, d)

  def awaitReady[T <: Awaitable[_]](a: T, d: Duration = 5.seconds): T =
    Await.ready(a, d)

  test("retries do not see the total timeout") {
    val stats = new InMemoryStatsReceiver()
    val params =
      Stack.Params.empty +
        param.Stats(stats) +
        Retries.Budget(RetryBudget.Infinite)
    val stackClient = TestStackClient(totalTimeoutStack, params)
    val methodBuilder = MethodBuilder.from("retry_it", stackClient)

    val client = methodBuilder.withTimeout
      .total(10.milliseconds)
      .withRetry
      .forClassifier {
        case ReqRep(_, Throw(_: GlobalRequestTimeoutException)) =>
          ResponseClass.RetryableFailure
      }
      .newService("a_client")

    intercept[GlobalRequestTimeoutException] {
      awaitResult(client(1))
    }
    // while we have a RetryFilter, the underlying service returns `Future.never`
    // and as such, the stats are never updated.
    assert(stats.stat("retry_it", "a_client", "retries")() == Seq.empty)
  }

  test("deadline stats are retrievable from timeout filter") {
    val stats = new InMemoryStatsReceiver()
    val params =
      Stack.Params.empty +
        param.Stats(stats)
    val stackClient = TestStackClient(totalTimeoutStack, params)
    val methodBuilder = MethodBuilder.from("timeout", stackClient)

    val deadline = Deadline.ofTimeout(10.milliseconds)

    val client = methodBuilder.withTimeout
      .total(2.seconds)
      .newService("a_client")

    Contexts.broadcast.let(Deadline, deadline) {
      intercept[GlobalRequestTimeoutException] {
        awaitResult(client(1))
      }
      assert(stats.stat("timeout", "a_client", "current_deadline")().size > 0)
    }
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

    // keep track of the number of requests initiated
    var tries = 0
    val svc: Service[Int, Int] = Service.mk { i =>
      tries += 1
      Future.sleep(perReqTimeout + 1.millis)(timer).map(_ => i)
    }

    val stack = TimeoutFilter
      .clientModule[Int, Int]
      .toStack(Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc)))
    val stackClient = TestStackClient(stack, params)
    val methodBuilder = MethodBuilder.from("together", stackClient)

    // the first 2 attempts will hit the per-request timeout, with each
    // being retried. then the the 3 attempt (2nd retry) should run into
    // the total timeout.
    val client = methodBuilder.withTimeout
      .perRequest(perReqTimeout)
      .withTimeout
      .total(totalTimeout)
      .withRetry
      .forClassifier {
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

      eventually {
        // wait for the first request to time out and trigger a retry (2 tries initiated)
        assert(tries == 2)
      }

      // hit the 2nd per-req timeout.
      tc.advance(perReqTimeout)
      timer.tick()
      assert(!rep.isDefined)

      eventually {
        // wait for the second request (first retry) to time out and trigger another retry
        assert(tries == 3)
      }

      // hit the total timeout
      tc.advance(20.milliseconds)
      timer.tick()
      assert(rep.isDefined)

      intercept[GlobalRequestTimeoutException] {
        awaitResult(rep)
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
        StatsFilter.Now(Some(Stopwatch.timeMillis)) +
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

    val stack = TimeoutFilter
      .clientModule[Int, Int]
      .toStack(Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc)))
    val stackClient = TestStackClient(stack, params)
    val methodBuilder = MethodBuilder.from("destination", stackClient)

    // the first attempts will hit the per-request timeout and will be
    // retried. then the retry should succeed.
    val methodName = "a_method"
    val client = methodBuilder.withTimeout
      .perRequest(perReqTimeout)
      .withRetry
      .forClassifier {
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
      awaitReady(rep)

      assert(2 == attempts.get)
      eventually {
        // confirm there was 1 retry
        assert(stats.stat(clientLabel, methodName, "retries")() == Seq(1))

        // the logical stats should only show 1 successful request despite
        // 2 "actual" requests
        assert(stats.counter(clientLabel, methodName, "logical", "requests")() == 1)
        assert(stats.counter(clientLabel, methodName, "logical", "success")() == 1)
        // and zero logical failures
        assert(stats.counter(clientLabel, methodName, "logical", "failures")() == 0)
        val latencies = stats.stat(clientLabel, methodName, "logical", "request_latency_ms")()
        assert(latencies.size == 1)
        assert(latencies.head <= (perReqTimeout + delta).inMillis)
      }

      val otherMethod = "other"
      val client2 = methodBuilder.newService(otherMethod)

      // issue the request
      val rep2 = client2(2)
      awaitReady(rep2)

      assert(3 == attempts.get)
      eventually {
        // the logical stats should be separate per-"method"
        assert(stats.counter(clientLabel, otherMethod, "logical", "requests")() == 1)
      }
    }
  }

  test("logical failure stats") {
    val stats = new InMemoryStatsReceiver()
    val clientLabel = "the_client"
    val params =
      Stack.Params.empty +
        param.Label(clientLabel) +
        param.Stats(stats)

    val failure = Failure("some reason", new RuntimeException("welp"))
      .withSource(Failure.Source.Service, "test_service")
    val svc: Service[Int, Int] = new FailedService(failure)

    val stack = Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc))
    val stackClient = TestStackClient(stack, params)

    val methodBuilder = MethodBuilder.from("destination", stackClient)

    // without method scope
    val client = methodBuilder.newService

    // issue a failing request
    intercept[Failure] {
      awaitResult(client(1))
    }

    eventually {
      assert(1 == stats.counters(Seq(clientLabel, "logical", "failures")))
    }

    // and with method scope
    val methodName = "a_method"
    val aMethodClient = methodBuilder.newService(methodName)

    // issue a failing request
    intercept[Failure] {
      awaitResult(aMethodClient(1))
    }

    eventually {
      assert(1 == stats.counters(Seq(clientLabel, methodName, "logical", "failures")))
    }
  }

  test("newServices with no MethodName are added to the Registry") {
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
        registry.filter { entry => entry.key.head == "client" }.toSet

      val totalSvc = methodBuilder.newService
      val totalSvcEntries = Set(
        Entry(key("statsReceiver"), s"InMemoryStatsReceiver/$clientName"),
        Entry(key("retry"), "Config(None,2)")
      )
      assert(filteredRegistry == totalSvcEntries)

      awaitResult(totalSvc.close())
      assert(Set.empty == filteredRegistry)
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
        registry.filter { entry => entry.key.head == "client" }.toSet

      // test a "vanilla" one
      val vanillaSvc = methodBuilder.newService("vanilla")
      val vanillaEntries = Set(
        Entry(key("vanilla", "statsReceiver"), s"InMemoryStatsReceiver/$clientName/vanilla"),
        Entry(key("vanilla", "retry"), "Config(None,2)")
      )
      assert(filteredRegistry == vanillaEntries)

      // test with retries disabled and timeouts
      val sundaeSvc = methodBuilder.withTimeout
        .total(10.seconds)
        .withTimeout
        .perRequest(1.second)
        .withRetry
        .disabled
        .newService("sundae")
      val sundaeEntries = Set(
        Entry(key("sundae", "statsReceiver"), s"InMemoryStatsReceiver/$clientName/sundae"),
        Entry(key("sundae", "retry"), "Config(Some(Disabled),2)"),
        Entry(
          key("sundae", "timeout", "total"),
          "TunableDuration(total,10.seconds,Tunable(com.twitter.util.tunable.NoneTunable))"
        ),
        Entry(
          key("sundae", "timeout", "per_request"),
          "TunableDuration(perRequest,1.seconds,Tunable(com.twitter.util.tunable.NoneTunable))"
        )
      )
      filteredRegistry should contain theSameElementsAs (vanillaEntries ++ sundaeEntries)

      val vanillaClose = vanillaSvc.close()
      assert(!vanillaClose.isDefined)
      filteredRegistry should contain theSameElementsAs sundaeEntries

      awaitReady(sundaeSvc.close())
      assert(vanillaClose.isDefined)
      assert(Set.empty == filteredRegistry)
    }
  }

  test("stats are filtered with methodName if it exists") {
    val stats = new InMemoryStatsReceiver()
    val clientLabel = "the_client"
    val params =
      Stack.Params.empty +
        param.Label(clientLabel) +
        param.Stats(stats)

    val failure = Failure("some reason", new RuntimeException("welp"))
      .withSource(Failure.Source.Service, "test_service")
    val svc: Service[Int, Int] = new FailedService(failure)

    val stack = Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc))
    val stackClient = TestStackClient(stack, params)

    val methodBuilder = MethodBuilder.from("destination", stackClient)

    val methodName = "a_method"
    val client = methodBuilder.newService(methodName)

    // issue a failing request
    intercept[Failure] {
      awaitResult(client(1))
    }

    eventually {
      assert(1 == stats.counters(Seq(clientLabel, methodName, "logical", "requests")))
      assert(
        1 == stats.counters(
          Seq(
            clientLabel,
            methodName,
            "logical",
            "failures",
            "com.twitter.finagle.Failure",
            "java.lang.RuntimeException"
          )
        )
      )
    }

    // verify the metrics are getting filtered down
    assert(!stats.gauges.contains(Seq(clientLabel, methodName, "logical", "pending")))
    val sourcedFailures = stats.counters.exists {
      case (names, _) =>
        names.containsSlice(Seq(clientLabel, methodName, "logical", "sourcedfailures"))
    }
    assert(!sourcedFailures)
  }

  test("stats are not filtered with methodName if it does not exist") {
    val stats = new InMemoryStatsReceiver()
    val clientLabel = "the_client"
    val params =
      Stack.Params.empty +
        param.Label(clientLabel) +
        param.Stats(stats)

    val failure = Failure("some reason", new RuntimeException("welp"))
      .withSource(Failure.Source.Service, "test_service")
    val svc: Service[Int, Int] = new FailedService(failure)

    val stack = Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc))
    val stackClient = TestStackClient(stack, params)

    val methodBuilder = MethodBuilder.from("destination", stackClient)

    val client = methodBuilder.newService

    // issue a failing request
    intercept[Failure] {
      awaitResult(client(1))
    }

    eventually {
      assert(1 == stats.counters(Seq(clientLabel, "logical", "requests")))
      assert(
        1 == stats.counters(
          Seq(
            clientLabel,
            "logical",
            "failures",
            "com.twitter.finagle.Failure",
            "java.lang.RuntimeException"
          )
        )
      )
    }

    // verify the metrics are getting filtered down
    assert(!stats.gauges.contains(Seq(clientLabel, "logical", "pending")))
    assert(!stats.gauges.contains(Seq(clientLabel, "logical", "pending")))
    val sourcedFailures = stats.counters.exists {
      case (names, _) =>
        names.containsSlice(Seq(clientLabel, "logical", "sourcedfailures"))
    }
    assert(!sourcedFailures)
  }

  test("makes stats lazily") {
    val stats = new InMemoryStatsReceiver()
    val clientLabel = "the_client"
    val params =
      Stack.Params.empty +
        param.Label(clientLabel) +
        param.Stats(stats)

    val failure = Failure("some reason", new RuntimeException("welp"))
      .withSource(Failure.Source.Service, "test_service")
    val svc: Service[Int, Int] = new FailedService(failure)

    val stack = Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc))
    val stackClient = TestStackClient(stack, params)

    val methodBuilder = MethodBuilder.from("destination", stackClient)

    val methodName = "a_method"
    val client = methodBuilder.newService(methodName)

    assert(stats.counters.keySet.filter(_.startsWith(Seq(clientLabel, methodName))).isEmpty)
    assert(stats.stats.keySet.filter(_.startsWith(Seq(clientLabel, methodName))).isEmpty)
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
      def status: Status = Status.Open
    }
    val stk = Stack.leaf(Stack.Role("test"), svcFac)
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
    intercept[ServiceClosedException] { awaitResult(m1(1)) }
    assert(m2.isAvailable)
    assert(svc.isAvailable)

    // validate that a second close of the same method does nothing
    assert(!m1Close.isDefined)
    assert(!m1.isAvailable)
    assert(m2.isAvailable)
    assert(svc.isAvailable)

    // validate that closing the last method closes the underlying service.
    awaitReady(m2.close())
    assert(m1Close.isDefined)
    assert(!m1.isAvailable)
    assert(!m2.isAvailable)
    intercept[ServiceClosedException] { awaitResult(m2(1)) }
    assert(!svc.isAvailable)
  }

  test("failures are annotated with the method name if it exists") {
    val params = Stack.Params.empty

    val failure = Failure("some reason", new RuntimeException("welp"))
    val svc: Service[Int, Int] = new FailedService(failure)

    val stack = Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc))
    val stackClient = TestStackClient(stack, params)

    val methodBuilder = MethodBuilder.from("destination", stackClient)

    val methodName = "a_method"
    val client = methodBuilder.newService(methodName)

    // issue a failing request
    val f = intercept[Failure] {
      awaitResult(client(1))
    }

    assert(f.getSource(Failure.Source.Method).contains(methodName))
  }

  test("failures are not annotated with the method name if it does not exist") {
    val params = Stack.Params.empty

    val failure = Failure("some reason", new RuntimeException("welp"))
    val svc: Service[Int, Int] = new FailedService(failure)

    val stack = Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc))
    val stackClient = TestStackClient(stack, params)

    val methodBuilder = MethodBuilder.from("destination", stackClient)

    val client = methodBuilder.newService

    // issue a failing request
    val f = intercept[Failure] {
      awaitResult(client(1))
    }

    assert(f.getSource(Failure.Source.Method).isEmpty)
  }

  test("nonIdempotent disables BackupRequestFilter") {
    val underlying = mock[Service[Int, Int]]
    val svc = ServiceFactory.const(underlying)

    val configuredBrfParam =
      BackupRequestFilter.Configured(maxExtraLoad = 1.percent, sendInterrupts = true)

    // Configure BackupRequestFilter
    val params = Stack.Params.empty

    val stack = Stack.leaf(Stack.Role("test"), svc)

    val stackClient = TestStackClient(stack, params)
    val initialMethodBuilder = MethodBuilder.from("with backups", stackClient)
    val methodBuilder =
      initialMethodBuilder.withConfig(initialMethodBuilder.config.copy(backup = configuredBrfParam))

    // Ensure BRF is configured before calling `nonIdempotent`
    assert(methodBuilder.config.backup == configuredBrfParam)

    val nonIdempotentClient = methodBuilder.nonIdempotent

    // Ensure BRF is disabled after calling `nonIdempotent`
    assert(nonIdempotentClient.config.backup == BackupRequestFilter.Disabled)
  }

  test("nonIdempotent client keeps existing ResponseClassifier in params ") {
    val underlying = mock[Service[Int, Int]]
    val svc = ServiceFactory.const(underlying)

    val configuredResponseClassifierParam =
      param.ResponseClassifier(ResponseClassifier.RetryOnTimeout)

    // Configure BackupRequestFilter
    val params = Stack.Params.empty + configuredResponseClassifierParam

    val stack = Stack.leaf(Stack.Role("test"), svc)

    val stackClient = TestStackClient(stack, params)
    val methodBuilder = MethodBuilder.from("with classifier", stackClient)

    // Ensure classifier is configured before calling `nonIdempotent`
    assert(methodBuilder.params[param.ResponseClassifier] == configuredResponseClassifierParam)

    val nonIdempotentClient = methodBuilder.nonIdempotent

    // Ensure classifier is still in params after calling `nonIdempotent`
    assert(
      nonIdempotentClient.params[param.ResponseClassifier] ==
        configuredResponseClassifierParam
    )
  }

  test("nonIdempotent sets ResponseClassifier.default for Retries") {
    val stats = new InMemoryStatsReceiver()
    val timer = new MockTimer()
    val classifier: ResponseClassifier = {
      case ReqRep(_, Throw(_)) => ResponseClass.RetryableFailure
    }
    val params =
      Stack.Params.empty +
        param.Timer(timer) +
        param.Stats(stats) +
        Retries.Budget(RetryBudget.Infinite) +
        param.ResponseClassifier(classifier)

    val svc: Service[Int, Int] = Service.mk { i =>
      if (i == 0) throw new Exception("boom!")
      else throw ChannelWriteException(new Exception("boom"))
    }

    val stack = Retries
      .moduleRequeueable[Int, Int]
      .toStack(Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc)))
    val stackClient = TestStackClient(stack, params)
    val mb = MethodBuilder.from("mb", stackClient)

    // ensure we *do* get retries with unchanged
    val client = mb.newService("a_client")

    val rep1 = client(0)
    intercept[Exception] {
      awaitResult(rep1, 1.second)
    }
    eventually {
      assert(stats.counters(Seq("mb", "a_client", "logical", "requests")) == 1)
      assert(stats.stat("mb", "a_client", "retries")() == Seq(2))
    }

    // ensure we *don't* get retries for non-WriteExceptions with idempotent client
    val nonIdempotentClient = mb.nonIdempotent.newService("a_client_nonidempotent")

    val rep2 = nonIdempotentClient(0)
    intercept[Exception] {
      awaitResult(rep2, 1.second)
    }
    eventually {
      assert(stats.counters(Seq("mb", "a_client_nonidempotent", "logical", "requests")) == 1)
      assert(stats.stat("mb", "a_client_nonidempotent", "retries")() == Seq(0))
    }

    // This request should trigger a WriteException and thus is retried/requeued in RequeueFilter,
    // per ResponseClassifier.default, but not retried in MethodBuilder's retries.
    val writeExceptionReq = nonIdempotentClient(1)
    intercept[Exception] {
      awaitResult(writeExceptionReq, 1.second)
    }
    eventually {
      assert(stats.counters(Seq("mb", "a_client_nonidempotent", "logical", "requests")) == 2)
      assert(stats.stat("mb", "a_client_nonidempotent", "retries")() == Seq(0, 0))
      // Requeued 20 times
      // (Infinite budget has balance of 100 * 0.2 max requeues per request = 20)
      assert(stats.stat("retries", "requeues_per_request")() == Seq(0, 0, 0, 0, 20))
    }
  }

  test("idempotent uses passed params to configure BackupRequestFilter/ResponseClassifier") {
    val stats = new InMemoryStatsReceiver()
    val timer = new MockTimer()
    val params =
      Stack.Params.empty +
        param.Timer(timer) +
        param.Stats(stats) +
        Retries.Budget(RetryBudget.Infinite)

    val perReqTimeout = 50.milliseconds
    val totalTimeout = perReqTimeout * 2 + 20.milliseconds
    val classifier: ResponseClassifier = ResponseClassifier.named("foo") {
      case ReqRep(_, Throw(_: IndividualRequestTimeoutException)) =>
        ResponseClass.RetryableFailure
    }

    // keep track of the number of requests initiated
    var tries = 0;
    val svc: Service[Int, Int] = Service.mk { i =>
      tries += 1
      Future.sleep(perReqTimeout + 1.millis)(timer).map(_ => i)
    }

    val stack = TimeoutFilter
      .clientModule[Int, Int]
      .toStack(Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc)))
    val stackClient = TestStackClient(stack, params)
    val mb = MethodBuilder
      .from("mb", stackClient)
      .withTimeout
      .perRequest(perReqTimeout)
      .withTimeout
      .total(totalTimeout)
      .idempotent(1.percent, sendInterrupts = true, classifier)

    mb.config.backup match {
      case BackupRequestFilter.Param
            .Configured(maxExtraLoadTunable, sendInterrupts, minSendBackupAfterMs) =>
        assert(
          maxExtraLoadTunable().get == 1.percent && sendInterrupts && minSendBackupAfterMs == 1)
      case _ => fail("BackupRequestFilter not configured")
    }
    assert(mb.config.retry.responseClassifier.toString == s"Idempotent($classifier)")

    // ensure that the response classifier was also used to configure retries

    val client = mb.newService("a_client")

    Time.withCurrentTimeFrozen { tc =>
      // issue the request
      val rep = client(1)
      assert(!rep.isDefined)

      // hit the 1st per-req timeout.
      tc.advance(perReqTimeout)
      timer.tick()
      assert(!rep.isDefined)

      eventually {
        // wait for the first request to time out and trigger a retry (2 tries initiated)
        assert(tries == 2)
      }

      // hit the 2nd per-req timeout.
      tc.advance(perReqTimeout)
      timer.tick()
      assert(!rep.isDefined)

      eventually {
        // wait for the second request (first retry) to time out and trigger another retry
        assert(tries == 3)
      }

      // hit the total timeout
      tc.advance(20.milliseconds)
      timer.tick()
      assert(rep.isDefined)

      intercept[GlobalRequestTimeoutException] {
        awaitResult(rep)
      }

      eventually {
        // confirm there were 2 retries issued
        assert(stats.stat("mb", "a_client", "retries")() == Seq(2))
      }
    }
  }

  test("idempotent that takes a Tunable uses it to configure BackupRequestFilter") {
    val underlying = mock[Service[Int, Int]]
    val svc = ServiceFactory.const(underlying)

    val tunable = Tunable.const("brfTunable", 50.percent)
    val stack = Stack.leaf(Stack.Role("test"), svc)

    val stackClient = TestStackClient(stack, Stack.Params.empty)
    val mb = MethodBuilder
      .from("with Tunable", stackClient)
      .idempotent(tunable, sendInterrupts = true, ResponseClassifier.Default)

    assert(
      mb.config.backup == BackupRequestFilter
        .Configured(tunable, sendInterrupts = true)
    )
  }

  test("method builder has different BackupRequestFilter configs when configured multiple times") {
    val underlying = mock[Service[Int, Int]]
    val svc = ServiceFactory.const(underlying)

    val tunable = Tunable.const("brfTunable", 50.percent)
    val stack = Stack.leaf(Stack.Role("test"), svc)

    val stackClient = TestStackClient(stack, Stack.Params.empty)
    val mb = MethodBuilder
      .from("with Tunable", stackClient)
      .idempotent(tunable, sendInterrupts = true, ResponseClassifier.Default)

    assert(
      mb.config.backup == BackupRequestFilter
        .Configured(tunable, sendInterrupts = true)
    )

    val nonIdempotentMB = mb.nonIdempotent
    assert(nonIdempotentMB.config.backup == BackupRequestFilter.Disabled)
  }

  test("idempotent combines existing classifier with new one") {
    val stats = new InMemoryStatsReceiver()
    val timer = new MockTimer()

    val myException1 = new Exception("boom1!")
    val myException2 = new Exception("boomz2")

    val existingClassifier: ResponseClassifier = {
      case ReqRep(0, Throw(_)) => ResponseClass.RetryableFailure
    }

    val newClassifier: ResponseClassifier = {
      case ReqRep(1, Throw(_)) => ResponseClass.RetryableFailure
    }

    val params =
      Stack.Params.empty +
        param.Timer(timer) +
        param.Stats(stats) +
        Retries.Budget(RetryBudget.Infinite) +
        param.ResponseClassifier(existingClassifier)

    val svc: Service[Int, Int] = Service.mk { i =>
      if (i == 0) throw myException1
      else throw myException2
    }

    val stack = Retries
      .moduleRequeueable[Int, Int]
      .toStack(Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc)))
    val stackClient = TestStackClient(stack, params)
    val client = MethodBuilder
      .from("mb", stackClient)
      .idempotent(1.percent, sendInterrupts = true, newClassifier)
      .newService("a_client")

    val rep1 = client(0)
    val exc1 = intercept[Exception] {
      awaitResult(rep1, 1.second)
    }
    assert(exc1 == myException1)

    // ensure exceptions that fall under existing classifier, and those that fall under the
    // new classifier, are retried
    eventually {
      assert(stats.counters(Seq("mb", "a_client", "logical", "requests")) == 1)
      assert(stats.stat("mb", "a_client", "retries")() == Seq(2))
    }

    val rep2 = client(1)
    val exc2 = intercept[Exception] {
      awaitResult(rep2, 1.second)
    }
    assert(exc2 == myException2)

    eventually {
      assert(stats.counters(Seq("mb", "a_client", "logical", "requests")) == 2)
      assert(stats.stat("mb", "a_client", "retries")() == Seq(2, 2))
    }
  }

  test("idempotent does not turn nonretryablefailures into retryablefailures") {
    val stats = new InMemoryStatsReceiver()
    val params = Stack.Params.empty +
      param.Stats(stats)
    val nonRetryingClassifier: ResponseClassifier = {
      case ReqRep(_, Throw(_)) => ResponseClass.NonRetryableFailure
    }
    val myException = new Exception("boom!")
    val svc: Service[Int, Int] = Service.mk { i => throw myException }
    val stack = Retries
      .moduleRequeueable[Int, Int]
      .toStack(Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc)))
    val stackClient = TestStackClient(stack, params)
    val client = MethodBuilder
      .from("mb", stackClient)
      .withRetry.forClassifier(nonRetryingClassifier)
      .idempotent(1.percent, sendInterrupts = true, { case _ if false => ??? }: ResponseClassifier)
      .newService("a_client")

    val rep = client(0)
    val exc = intercept[Exception] {
      awaitResult(rep, 1.second)
    }
    assert(exc == myException)
    // There should be no retries because the configured classifier marked all Throws as
    // non-retryable
    eventually {
      assert(stats.counters(Seq("mb", "a_client", "logical", "requests")) == 1)
      assert(stats.stat("mb", "a_client", "retries")() == Seq(0))
    }
  }

  test("nonidempotent turns retryablefailures into nonretryablefailures") {
    val stats = new InMemoryStatsReceiver()
    val params = Stack.Params.empty +
      param.Stats(stats)
    val nonRetryingClassifier: ResponseClassifier = {
      case ReqRep(_, Throw(_)) => ResponseClass.RetryableFailure
    }
    val myException = new Exception("boom!")
    val svc: Service[Int, Int] = Service.mk { i => throw myException }
    val stack = Retries
      .moduleRequeueable[Int, Int]
      .toStack(Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc)))
    val stackClient = TestStackClient(stack, params)
    val client = MethodBuilder
      .from("mb", stackClient)
      .withRetry.forClassifier(nonRetryingClassifier)
      .nonIdempotent
      .newService("a_client")

    val rep = client(0)
    val exc = intercept[Exception] {
      awaitResult(rep, 1.second)
    }
    assert(exc == myException)
    eventually {
      assert(stats.counters(Seq("mb", "a_client", "logical", "requests")) == 1)
      assert(stats.stat("mb", "a_client", "retries")() == Seq(0))
    }
  }

  test("idempotent preserves unusual classifications") {
    val stats = new InMemoryStatsReceiver()
    val params = Stack.Params.empty +
      param.Stats(stats)

    // throw is success, and return is failure
    val nonRetryingClassifier: ResponseClassifier = {
      case ReqRep(0, Throw(_)) => ResponseClass.Success
      case ReqRep(1, Return(_)) => ResponseClass.NonRetryableFailure
    }
    val myException = new Exception("boom!")
    val svc: Service[Int, Int] = Service.mk { i =>
      if (i == 0) throw myException
      else Future.value(i)
    }
    val stack = Retries
      .moduleRequeueable[Int, Int]
      .toStack(Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc)))
    val stackClient = TestStackClient(stack, params)
    val client = MethodBuilder
      .from("mb", stackClient)
      .withRetry.forClassifier(nonRetryingClassifier)
      .idempotent(1.percent, sendInterrupts = true, { case _ if false => ??? }: ResponseClassifier)
      .newService("a_client")

    val rep1 = client(0)
    val exc = intercept[Exception] {
      awaitResult(rep1, 1.second)
    }
    assert(exc == myException)
    eventually {
      assert(stats.counters(Seq("mb", "a_client", "logical", "requests")) == 1)
      assert(stats.counters(Seq("mb", "a_client", "logical", "success")) == 1)
      assert(stats.stat("mb", "a_client", "retries")() == Seq(0))
    }

    val rep2 = client(1)
    val result = awaitResult(rep2, 1.second)
    assert(result == 1)
    eventually {
      assert(stats.counters(Seq("mb", "a_client", "logical", "requests")) == 2)
      assert(stats.counters(Seq("mb", "a_client", "logical", "success")) == 1)
      assert(stats.stat("mb", "a_client", "retries")() == Seq(0, 0))
    }
  }

  test("nonidempotent preserves unusual classifications") {
    val stats = new InMemoryStatsReceiver()
    val params = Stack.Params.empty +
      param.Stats(stats)

    // throw is success, and return is failure
    val nonRetryingClassifier: ResponseClassifier = {
      case ReqRep(0, Throw(_)) => ResponseClass.Success
      case ReqRep(1, Return(_)) => ResponseClass.NonRetryableFailure
    }
    val myException = new Exception("boom!")
    val svc: Service[Int, Int] = Service.mk { i =>
      if (i == 0) throw myException
      else Future.value(i)
    }
    val stack = Retries
      .moduleRequeueable[Int, Int]
      .toStack(Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc)))
    val stackClient = TestStackClient(stack, params)
    val client = MethodBuilder
      .from("mb", stackClient)
      .withRetry.forClassifier(nonRetryingClassifier)
      .nonIdempotent
      .newService("a_client")

    val rep1 = client(0)
    val exc = intercept[Exception] {
      awaitResult(rep1, 1.second)
    }
    assert(exc == myException)
    eventually {
      assert(stats.counters(Seq("mb", "a_client", "logical", "requests")) == 1)
      assert(stats.counters(Seq("mb", "a_client", "logical", "success")) == 1)
      assert(stats.stat("mb", "a_client", "retries")() == Seq(0))
    }

    val rep2 = client(1)
    val result = awaitResult(rep2, 1.second)
    assert(result == 1)
    eventually {
      assert(stats.counters(Seq("mb", "a_client", "logical", "requests")) == 2)
      assert(stats.counters(Seq("mb", "a_client", "logical", "success")) == 1)
      assert(stats.stat("mb", "a_client", "retries")() == Seq(0, 0))
    }
  }

  test("idempotent does not remove any existing RetryBudget from params") {
    val retryBudget = RetryBudget(5.seconds, 10, 0.1)
    val params =
      Stack.Params.empty +
        Retries.Budget(retryBudget)

    val svc: Service[Int, Int] = Service.const(Future.value(1))

    val stack = Retries
      .moduleRequeueable[Int, Int]
      .toStack(Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc)))
    val stackClient = TestStackClient(stack, params)
    val client = MethodBuilder
      .from("mb", stackClient)
      .idempotent(1.percent, sendInterrupts = true, ResponseClassifier.Default)

    assert(client.params[Retries.Budget].retryBudget eq retryBudget)
  }

  test("shares RetryBudget between methods") {
    val params = StackClient.defaultParams

    val svc: Service[Int, Int] = Service.const(Future.value(1))

    val stack = Retries
      .moduleRequeueable[Int, Int]
      .toStack(Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc)))
    val stackClient = TestStackClient(stack, params)
    val client = MethodBuilder.from("mb", stackClient)

    val method1 =
      client.idempotent(1.percent, sendInterrupts = true, ResponseClassifier.Default)

    val method2 =
      client.idempotent(1.percent, sendInterrupts = true, ResponseClassifier.Default)

    // each method should share the same retryBudget
    assert(
      method1.params[Retries.Budget].retryBudget eq
        method2.params[Retries.Budget].retryBudget
    )
  }

  test("withFilter applies filter for each endpoint service") {
    val svc = new Service[Int, Int] {
      def apply(request: Int): Future[Int] = Future.value(request)
    }

    val filterBoom: Filter.TypeAgnostic = new Filter.TypeAgnostic {
      def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = new SimpleFilter[Req, Rep] {
        def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
          Future.exception(new Exception)
        }
      }
    }

    val called = new AtomicInteger(0)
    val filterCalled: Filter.TypeAgnostic = new Filter.TypeAgnostic {
      def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = new SimpleFilter[Req, Rep] {
        def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
          called.addAndGet(1)
          service(request)
        }
      }
    }

    val stackClient =
      TestStackClient(Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc)), Stack.Params.empty)
    val methodBuilder = MethodBuilder.from("withFilter", stackClient)

    val clientA = methodBuilder.filtered(filterBoom).newService("a_client")
    val clientB = methodBuilder.filtered(filterCalled).newService("b_client")

    intercept[Exception](awaitResult(clientA(1)))
    awaitResult(clientB(1))
    assert(called.get() == 1)
  }
}
