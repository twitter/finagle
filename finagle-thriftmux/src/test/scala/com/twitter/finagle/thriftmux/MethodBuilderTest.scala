package com.twitter.finagle.thriftmux

import com.twitter.conversions.DurationOps._
import com.twitter.conversions.PercentOps._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.context.Deadline
import com.twitter.finagle.mux.Request
import com.twitter.finagle.mux.Response
import com.twitter.finagle.service.DeadlineOnlyToggle
import com.twitter.finagle.service.ReqRep
import com.twitter.finagle.service.ResponseClass
import com.twitter.finagle.stats._
import com.twitter.finagle.thriftmux.thriftscala.InvalidQueryException
import com.twitter.finagle.thriftmux.thriftscala.TestService
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.util.Showable
import com.twitter.scrooge
import com.twitter.util._
import com.twitter.util.registry.Entry
import com.twitter.util.registry.GlobalRegistry
import com.twitter.util.registry.SimpleRegistry
import com.twitter.util.tunable.Tunable
import java.net.InetAddress
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import org.scalatest.concurrent.Eventually
import scala.collection.JavaConverters._
import org.scalatest.funsuite.AnyFunSuite

class MethodBuilderTest extends AnyFunSuite with Eventually {

  private class SlowTestService(implicit val timer: Timer) extends TestService.MethodPerEndpoint {
    def query(x: String): Future[String] = {
      Future.sleep(50.millis).before { Future.value(x) }
    }
    def question(y: String): Future[String] = {
      Future.sleep(50.millis).before { Future.value(y) }
    }
    def inquiry(z: String): Future[String] = {
      Future.sleep(50.millis).before { Future.value(z) }
    }
  }

  def await[T](a: Awaitable[T], d: Duration = 5.seconds): T =
    Await.result(a, d)

  def serverImpl: ThriftMux.Server = {
    // need to copy the params since the `.server` call sets the Label to "thrift" into
    // the current muxers params
    val serverParams = ThriftMux.server.params
    ThriftMux.server.copy(muxer = ThriftMux.Server.defaultMuxer.withParams(serverParams))
  }

  def clientImpl: ThriftMux.Client =
    ThriftMux.client.copy(muxer = ThriftMux.Client.standardMuxer)

  private def testMethodBuilderTimeouts(
    stats: InMemoryStatsReceiver,
    server: ListeningServer,
    builder: MethodBuilder
  ): Unit = {
    // these should never complete within the timeout
    // ServicePerEndpoint
    val shortTimeoutSvcPerEndpoint: Service[TestService.Query.Args, TestService.Query.SuccessType] =
      builder
        .withTimeoutPerRequest(5.millis)
        .servicePerEndpoint[TestService.ServicePerEndpoint]("fast")
        .query

    intercept[IndividualRequestTimeoutException] {
      await(shortTimeoutSvcPerEndpoint(TestService.Query.Args("shorty")))
    }

    eventually {
      assert(stats.counter("a_label", "fast", "logical", "requests")() == 1)
      assert(stats.counter("a_label", "fast", "logical", "success")() == 0)
    }

    // ReqRepServicePerEndpoint
    val shortTimeoutReqRepSvcPerEndpoint: Service[scrooge.Request[
      TestService.Query.Args
    ], scrooge.Response[
      TestService.Query.SuccessType
    ]] =
      builder
        .withTimeoutPerRequest(5.millis)
        .servicePerEndpoint[TestService.ReqRepServicePerEndpoint]("fast")
        .query

    intercept[IndividualRequestTimeoutException] {
      await(shortTimeoutReqRepSvcPerEndpoint(scrooge.Request(TestService.Query.Args("shorty"))))
    }

    // these should always complete within the timeout
    // ServicePerEndpoint
    val longTimeoutSvcPerEndpoint =
      builder
        .withTimeoutPerRequest(5.seconds)
        .servicePerEndpoint[TestService.ServicePerEndpoint]("slow")
        .query

    var result = await(longTimeoutSvcPerEndpoint(TestService.Query.Args("looong")))
    assert("looong" == result)
    eventually {
      assert(stats.counter("a_label", "slow", "logical", "requests")() == 1)
      assert(stats.counter("a_label", "slow", "logical", "success")() == 1)
    }
    // ReqRepServicePerEndpoint
    val longTimeoutReqRepSvcPerEndpoint =
      builder
        .withTimeoutPerRequest(5.seconds)
        .servicePerEndpoint[TestService.ReqRepServicePerEndpoint]("slow")
        .query

    val response = await(
      longTimeoutReqRepSvcPerEndpoint(scrooge.Request(TestService.Query.Args("looong")))
    )
    assert("looong" == response.value)

    await(server.close())
  }

  test("methodBuilder timeouts from Stack") {
    implicit val timer: Timer = DefaultTimer
    val service = new SlowTestService
    val server =
      serverImpl.serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), service)

    val stats = new InMemoryStatsReceiver()
    val client = clientImpl
      .configured(param.Timer(timer))
      .withStatsReceiver(stats)
      .withLabel("a_label")
    val name = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
    val builder: MethodBuilder = client.methodBuilder(name)

    testMethodBuilderTimeouts(stats, server, builder)
  }

  test("methodBuilder timeouts from ClientBuilder") {
    implicit val timer: Timer = DefaultTimer
    val service = new SlowTestService
    val server =
      serverImpl.serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), service)

    val stats = new InMemoryStatsReceiver()
    val client = clientImpl
      .configured(param.Timer(timer))

    val clientBuilder = ClientBuilder()
      .reportTo(stats)
      .name("a_label")
      .stack(client)
      .dest(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])))
    val mb = MethodBuilder.from(clientBuilder)

    testMethodBuilderTimeouts(stats, server, mb)
  }

  private def toggleOnCtx(fn: => Unit): Unit = {
    DeadlineOnlyToggle.unsafeOverride(Some(true))
    try fn
    finally DeadlineOnlyToggle.unsafeOverride(None)
  }

  test("methodBuilder prefers deadlines when they are in the context") {
    implicit val timer: Timer = DefaultTimer
    val service = new SlowTestService
    val server =
      serverImpl.serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), service)

    val stats = new InMemoryStatsReceiver()
    val client = clientImpl
      .withStatsReceiver(stats)
      .configured(param.Timer(timer))

    val mb = client
      .withLabel("a_label")
      .methodBuilder(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])))

    val shortTimeoutSvcPerEndpoint: Service[TestService.Query.Args, TestService.Query.SuccessType] =
      mb.withTimeoutTotal(1000.millis) // very large timeout that should never fire
        .servicePerEndpoint[TestService.ServicePerEndpoint]("fast")
        .query

    toggleOnCtx {
      Contexts.broadcast.let(Deadline, Deadline.ofTimeout(5.millis)) {
        // short deadline should fire
        intercept[GlobalRequestTimeoutException] {
          await(shortTimeoutSvcPerEndpoint(TestService.Query.Args("shorty")))
        }
      }
    }

    eventually {
      assert(stats.counter("a_label", "fast", "deadline_only")() == 1)
    }
  }

  test("methodBuilder timeouts from configured ClientBuilder") {
    implicit val timer = new MockTimer
    val sleepTime = new AtomicReference[Duration](Duration.Bottom)
    val service = new TestService.MethodPerEndpoint {
      def query(x: String): Future[String] = {
        Future.sleep(sleepTime.get)(DefaultTimer).before { Future.value(x) }
      }
      def question(y: String): Future[String] = {
        Future.sleep(sleepTime.get)(DefaultTimer).before { Future.value(y) }
      }
      def inquiry(z: String): Future[String] = {
        Future.sleep(sleepTime.get)(DefaultTimer).before { Future.value(z) }
      }
    }
    val server =
      serverImpl.serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), service)

    val stats = new InMemoryStatsReceiver()
    val client = clientImpl
      .configured(param.Timer(timer))

    val clientBuilder = ClientBuilder()
    // set tight "default" timeouts that MB must override in
    // order to get successful responses.
      .requestTimeout(1.milliseconds)
      .timeout(2.milliseconds)
      .reportTo(stats)
      .name("a_label")
      .stack(client)
      .dest(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])))
    val mb = MethodBuilder.from(clientBuilder)

    Time.withCurrentTimeFrozen { control =>
      // these should never complete within the timeout
      // ServicePerEndpoint
      val asIsSvcPerEndpoint: Service[TestService.Query.Args, TestService.Query.SuccessType] =
        mb.servicePerEndpoint[TestService.ServicePerEndpoint]("as_is").query

      // Send a priming request to get a connection established so we don't timeout service acquisition
      assert("prime" == await(asIsSvcPerEndpoint(TestService.Query.Args("prime"))))
      sleepTime.set(Duration.Top)

      val req1 = asIsSvcPerEndpoint(TestService.Query.Args("nope"))
      control.advance(10.milliseconds)
      timer.tick()
      intercept[RequestTimeoutException] { await(req1) }
      eventually {
        assert(stats.counter("a_label", "as_is", "logical", "requests")() == 2)
        assert(stats.counter("a_label", "as_is", "logical", "success")() == 1)
      }

      // ReqRepServicePerEndpoint
      val asIsReqRepSvcPerEndpoint: Service[scrooge.Request[
        TestService.Query.Args
      ], scrooge.Response[
        TestService.Query.SuccessType
      ]] =
        mb.servicePerEndpoint[TestService.ReqRepServicePerEndpoint]("as_is").query
      val req2 = asIsReqRepSvcPerEndpoint(scrooge.Request(TestService.Query.Args("nope")))
      control.advance(10.milliseconds)
      timer.tick()
      intercept[RequestTimeoutException] { await(req2) }

      // increase the timeouts via MB and now the request should succeed
      // ServicePerEndpoint
      val longTimeoutSvcPerEndpoint: Service[
        TestService.Query.Args,
        TestService.Query.SuccessType
      ] =
        mb.withTimeoutPerRequest(5.seconds)
          .withTimeoutTotal(5.seconds)
          .servicePerEndpoint[TestService.ServicePerEndpoint]("good")
          .query

      // An actual sleep
      sleepTime.set(50.milliseconds)

      val req3 = longTimeoutSvcPerEndpoint(TestService.Query.Args("yep"))
      control.advance(1.second)
      timer.tick()

      val result = await(req3)
      assert("yep" == result)
      eventually {
        assert(stats.counter("a_label", "good", "logical", "requests")() == 1)
        assert(stats.counter("a_label", "good", "logical", "success")() == 1)
      }

      // ReqRepServicePerEndpoint
      val longTimeoutReqRepSvcPerEndpoint: Service[scrooge.Request[
        TestService.Query.Args
      ], scrooge.Response[
        TestService.Query.SuccessType
      ]] =
        mb.withTimeoutPerRequest(5.seconds)
          .withTimeoutTotal(5.seconds)
          .servicePerEndpoint[TestService.ReqRepServicePerEndpoint]("good")
          .query

      val req4 = longTimeoutReqRepSvcPerEndpoint(scrooge.Request(TestService.Query.Args("yep")))
      control.advance(1.second)
      timer.tick()
      val response = await(req4)
      assert("yep" == response.value)

      await(server.close())
    }
  }

  test("methodBuilder tunable timeouts from configured ClientBuilder") {
    val timer: MockTimer = new MockTimer()
    val service = new TestService.MethodPerEndpoint {
      private val methodCalled = new AtomicBoolean(false)
      def query(x: String): Future[String] = {
        if (methodCalled.compareAndSet(false, true)) {
          Future.value(x)
        } else {
          Future.never
        }
      }
      def question(y: String): Future[String] = {
        if (methodCalled.compareAndSet(false, true)) {
          Future.value(y)
        } else {
          Future.never
        }
      }
      def inquiry(z: String): Future[String] = {
        if (methodCalled.compareAndSet(false, true)) {
          Future.value(z)
        } else {
          Future.never
        }
      }
    }
    val server =
      serverImpl.serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), service)

    val stats = new InMemoryStatsReceiver()
    val client = clientImpl
      .configured(param.Timer(timer))

    val clientBuilder = ClientBuilder()
    // set tight "default" timeouts that MB must override in
    // order to get successful responses.
      .requestTimeout(1.milliseconds)
      .timeout(2.milliseconds)
      .reportTo(stats)
      .name("a_label")
      .stack(client)
      .dest(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])))
    val mb = MethodBuilder.from(clientBuilder)

    val perRequestTimeoutTunable = Tunable.emptyMutable[Duration]("perRequest")
    val totalTimeoutTunable = Tunable.emptyMutable[Duration]("total")
    val tunableTimeoutSvc: Service[TestService.Query.Args, TestService.Query.SuccessType] =
      mb.withTimeoutPerRequest(perRequestTimeoutTunable)
        .withTimeoutTotal(totalTimeoutTunable)
        .servicePerEndpoint[TestService.ServicePerEndpoint]("good")
        .query

    Time.withCurrentTimeFrozen { _ =>
      // send a good response to ensure the stack has a chance to initialized
      await(tunableTimeoutSvc(TestService.Query.Args("first")))
    }

    // increase the timeouts via MB and tests should fail after tunable timeouts
    Time.withCurrentTimeFrozen { currentTime =>
      // ------------------ test total timeouts ------------------
      perRequestTimeoutTunable.set(100.seconds) // long timeout that does not trigger
      totalTimeoutTunable.set(5.seconds)
      val result1 = tunableTimeoutSvc(TestService.Query.Args("yep"))
      assert(!result1.isDefined)

      // go past the total timeout
      currentTime.advance(6.seconds)
      timer.tick()
      val ex1 = intercept[GlobalRequestTimeoutException] { await(result1) }
      assert(ex1.getMessage().contains(totalTimeoutTunable().get.toString))

      // change the timeout
      totalTimeoutTunable.set(3.seconds)
      val result2 = tunableTimeoutSvc(TestService.Query.Args("nope"))
      assert(result2.poll == None)

      // this time, 4 seconds pushes us past
      currentTime.advance(4.seconds)
      timer.tick()
      val ex2 = intercept[GlobalRequestTimeoutException] { await(result2) }
      assert(ex2.getMessage().contains(totalTimeoutTunable().get.toString))

    }

    Time.withCurrentTimeFrozen { currentTime =>
      // ------------------ test per request timeouts ------------------
      totalTimeoutTunable.set(100.seconds) // long timeout that does not trigger
      perRequestTimeoutTunable.set(2.seconds)
      val result1 = tunableTimeoutSvc(TestService.Query.Args("huh"))
      assert(result1.poll == None)

      // go past the per request timeout
      currentTime.advance(5.seconds)
      timer.tick()
      val ex1 = intercept[IndividualRequestTimeoutException] { await(result1) }
      assert(ex1.getMessage().contains(perRequestTimeoutTunable().get.toString))

      // change the timeout
      perRequestTimeoutTunable.set(1.seconds)
      val result2 = tunableTimeoutSvc(TestService.Query.Args("what"))
      assert(result2.poll == None)

      // this time, 4 seconds pushes us past
      currentTime.advance(4.seconds)
      timer.tick()
      val ex2 = intercept[IndividualRequestTimeoutException] { await(result2) }
      assert(ex2.getMessage().contains(perRequestTimeoutTunable().get.toString))
    }

    await(server.close())
  }

  private[this] def testMethodBuilderRetries(
    stats: InMemoryStatsReceiver,
    server: ListeningServer,
    builder: MethodBuilder
  ): Unit = {
    // ServicePerEndpoint
    val retryInvalidSvcPerEndpoint: Service[TestService.Query.Args, TestService.Query.SuccessType] =
      builder
        .withRetryForClassifier {
          case ReqRep(_, Throw(InvalidQueryException(_))) =>
            ResponseClass.RetryableFailure
        }
        .servicePerEndpoint[TestService.ServicePerEndpoint]("all_invalid")
        .query

    intercept[InvalidQueryException] {
      await(retryInvalidSvcPerEndpoint(TestService.Query.Args("fail0")))
    }
    eventually {
      assert(stats.counter("a_label", "all_invalid", "logical", "requests")() == 1)
      assert(stats.counter("a_label", "all_invalid", "logical", "success")() == 0)
      assert(stats.stat("a_label", "all_invalid", "retries")() == Seq(2))
    }

    stats.clear()
    // ServicePerEndpoint retry configuration
    val retryInvalidSvcPerEndpointCustom: Service[
      TestService.Query.Args,
      TestService.Query.SuccessType
    ] =
      builder
        .withRetryForClassifier {
          case ReqRep(_, Throw(InvalidQueryException(_))) =>
            ResponseClass.RetryableFailure
        }
        .withMaxRetries(5)
        .servicePerEndpoint[TestService.ServicePerEndpoint]("all_invalid")
        .query

    intercept[InvalidQueryException] {
      await(retryInvalidSvcPerEndpointCustom(TestService.Query.Args("fail0")))
    }
    eventually {
      assert(stats.counter("a_label", "all_invalid", "logical", "requests")() == 1)
      assert(stats.counter("a_label", "all_invalid", "logical", "success")() == 0)
      assert(stats.stat("a_label", "all_invalid", "retries")() == Seq(5))
    }

    // ReqRepServicePerEndpoint
    val retryInvalidReqRepSvcPerEndpoint: Service[scrooge.Request[
      TestService.Query.Args
    ], scrooge.Response[
      TestService.Query.SuccessType
    ]] =
      builder
        .withRetryForClassifier {
          case ReqRep(_, Throw(InvalidQueryException(_))) =>
            ResponseClass.RetryableFailure
        }
        .servicePerEndpoint[TestService.ReqRepServicePerEndpoint]("all_invalid")
        .query

    intercept[InvalidQueryException] {
      await(retryInvalidReqRepSvcPerEndpoint(scrooge.Request(TestService.Query.Args("fail0"))))
    }

    // ServicePerEndpoint
    val errCode1SucceedsSvcPerEndpoint: Service[
      TestService.Query.Args,
      TestService.Query.SuccessType
    ] =
      builder
        .withRetryForClassifier {
          case ReqRep(_, Throw(InvalidQueryException(errorCode))) if errorCode == 0 =>
            ResponseClass.NonRetryableFailure
          case ReqRep(_, Throw(InvalidQueryException(errorCode))) if errorCode == 1 =>
            ResponseClass.Success
        }
        .servicePerEndpoint[TestService.ServicePerEndpoint]("err_1")
        .query

    intercept[InvalidQueryException] {
      // this is a non-retryable failure
      await(errCode1SucceedsSvcPerEndpoint(TestService.Query.Args("fail0")))
    }
    intercept[InvalidQueryException] {
      // this is a "successful" "failure"
      await(errCode1SucceedsSvcPerEndpoint(TestService.Query.Args("fail1")))
    }
    eventually {
      assert(stats.counter("a_label", "err_1", "logical", "requests")() == 2)
      assert(stats.counter("a_label", "err_1", "logical", "success")() == 1)
    }

    // ReqRepServicePerEndpoint
    val errCode1SucceedsReqRepSvcPerEndpoint: Service[scrooge.Request[
      TestService.Query.Args
    ], scrooge.Response[
      TestService.Query.SuccessType
    ]] =
      builder
        .withRetryForClassifier {
          case ReqRep(_, Throw(InvalidQueryException(errorCode))) if errorCode == 0 =>
            ResponseClass.NonRetryableFailure
          case ReqRep(_, Throw(InvalidQueryException(errorCode))) if errorCode == 1 =>
            ResponseClass.Success
        }
        .servicePerEndpoint[TestService.ReqRepServicePerEndpoint]("err_1")
        .query

    intercept[InvalidQueryException] {
      // this is a non-retryable failure
      await(errCode1SucceedsReqRepSvcPerEndpoint(scrooge.Request(TestService.Query.Args("fail0"))))
    }
    intercept[InvalidQueryException] {
      // this is a "successful" "failure"
      await(errCode1SucceedsReqRepSvcPerEndpoint(scrooge.Request(TestService.Query.Args("fail1"))))
    }

    await(server.close())
  }

  test("methodBuilder retries from Stack") {
    val service = new TestService.MethodPerEndpoint {
      def query(x: String): Future[String] = x match {
        case "fail0" => Future.exception(InvalidQueryException(0))
        case "fail1" => Future.exception(InvalidQueryException(1))
        case _ => Future.value(x)
      }
      def question(y: String): Future[String] = y match {
        case "fail0" => Future.exception(InvalidQueryException(0))
        case "fail1" => Future.exception(InvalidQueryException(1))
        case _ => Future.value(y)
      }
      def inquiry(z: String): Future[String] = z match {
        case "fail0" => Future.exception(InvalidQueryException(0))
        case "fail1" => Future.exception(InvalidQueryException(1))
        case _ => Future.value(z)
      }
    }
    val server =
      serverImpl.serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), service)
    val stats = new InMemoryStatsReceiver()
    val client = clientImpl
      .withStatsReceiver(stats)
      .withLabel("a_label")
    val name = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
    val builder: MethodBuilder = client.methodBuilder(name)

    testMethodBuilderRetries(stats, server, builder)
  }

  test("methodBuilder retries from Stack with MethodPerEndpoint") {
    val service = new TestService.MethodPerEndpoint {
      def query(x: String): Future[String] = x match {
        case "fail0" => Future.exception(InvalidQueryException(0))
        case "fail1" => Future.exception(InvalidQueryException(1))
        case _ => Future.value(x)
      }
      def question(y: String): Future[String] = y match {
        case "fail0" => Future.exception(InvalidQueryException(0))
        case "fail1" => Future.exception(InvalidQueryException(1))
        case _ => Future.value(y)
      }
      def inquiry(z: String): Future[String] = z match {
        case "fail0" => Future.exception(InvalidQueryException(0))
        case "fail1" => Future.exception(InvalidQueryException(1))
        case _ => Future.value(z)
      }
    }
    val server =
      serverImpl.serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), service)
    val stats = new InMemoryStatsReceiver()
    val client = clientImpl
      .withStatsReceiver(stats)
      .withLabel("a_label")
    val name = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
    val builder: MethodBuilder = client.methodBuilder(name)

    testMethodBuilderRetries(stats, server, builder)
  }

  test("methodBuilder retries from ClientBuilder") {
    val service = new TestService.MethodPerEndpoint {
      def query(x: String): Future[String] = x match {
        case "fail0" => Future.exception(InvalidQueryException(0))
        case "fail1" => Future.exception(InvalidQueryException(1))
        case _ => Future.value(x)
      }
      def question(y: String): Future[String] = y match {
        case "fail0" => Future.exception(InvalidQueryException(0))
        case "fail1" => Future.exception(InvalidQueryException(1))
        case _ => Future.value(y)
      }
      def inquiry(z: String): Future[String] = z match {
        case "fail0" => Future.exception(InvalidQueryException(0))
        case "fail1" => Future.exception(InvalidQueryException(1))
        case _ => Future.value(z)
      }
    }
    val server =
      serverImpl.serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), service)
    val stats = new InMemoryStatsReceiver()
    val client = clientImpl
    val clientBuilder = ClientBuilder()
      .reportTo(stats)
      .name("a_label")
      .stack(client)
      .dest(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])))

    val builder: MethodBuilder = MethodBuilder.from(clientBuilder)

    testMethodBuilderRetries(stats, server, builder)
  }

  test("methodBuilder retries from ClientBuilder with MethodPerEndpoint") {
    val service = new TestService.MethodPerEndpoint {
      def query(x: String): Future[String] = x match {
        case "fail0" => Future.exception(InvalidQueryException(0))
        case "fail1" => Future.exception(InvalidQueryException(1))
        case _ => Future.value(x)
      }
      def question(y: String): Future[String] = y match {
        case "fail0" => Future.exception(InvalidQueryException(0))
        case "fail1" => Future.exception(InvalidQueryException(1))
        case _ => Future.value(y)
      }
      def inquiry(z: String): Future[String] = z match {
        case "fail0" => Future.exception(InvalidQueryException(0))
        case "fail1" => Future.exception(InvalidQueryException(1))
        case _ => Future.value(z)
      }
    }
    val server =
      serverImpl.serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), service)
    val stats = new InMemoryStatsReceiver()
    val client = clientImpl
    val clientBuilder = ClientBuilder()
      .reportTo(stats)
      .name("a_label")
      .stack(client)
      .retries(20) // this should be ignored
      .dest(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])))

    val builder: MethodBuilder = MethodBuilder.from(clientBuilder)

    testMethodBuilderRetries(stats, server, builder)
  }

  test("methodBuilder stats are not eager for all methods") {
    // note that this CaptureStatsReceiver could be avoided if `InMemoryStatsReceiver`
    // was eager about creating counters and stats that have not been used.
    // see CSL-6751
    val metrics = new ConcurrentHashMap[String, Unit]()
    class CaptureStatsReceiver(protected val self: StatsReceiver) extends StatsReceiverProxy {

      override def counter(metricBuilder: MetricBuilder): Counter = {
        metrics.put(metricBuilder.name.mkString("/"), ())
        super.counter(metricBuilder)
      }

      override def stat(metricBuilder: MetricBuilder): Stat = {
        metrics.put(metricBuilder.name.mkString("/"), ())
        super.stat(metricBuilder)
      }

      override def addGauge(metricBuilder: MetricBuilder)(f: => Float): Gauge = {
        metrics.put(metricBuilder.name.mkString("/"), ())
        super.addGauge(metricBuilder)(f)
      }
    }

    val stats = new InMemoryStatsReceiver

    val service = new TestService.MethodPerEndpoint {
      def query(x: String): Future[String] = Future.value(x)
      def question(y: String): Future[String] = Future.value(y)
      def inquiry(z: String): Future[String] = Future.value(z)
    }

    val server =
      serverImpl.serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), service)

    val client = clientImpl
      .withStatsReceiver(new CaptureStatsReceiver(stats))
      .withLabel("a_service")
    val name = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
    val builder: MethodBuilder = client.methodBuilder(name)

    // ensure there are no metrics to start with. e.g. "logical", "query"
    assert(!metrics.keys.asScala.exists(key => key.contains("logical/requests")))

    // ensure that materializing into a ServicePerEndpoint doesn't create the metrics
    val spe = builder.servicePerEndpoint[TestService.ServicePerEndpoint]("a_method")
    assert(!metrics.keys.asScala.exists(key => key.contains("a_service/a_method")))

    // ensure that changing configuration doesn't either.
    val configured = builder
      .idempotent(0.1)
      .servicePerEndpoint[TestService.ServicePerEndpoint]("a_method")
      .query
    assert(!metrics.keys.asScala.exists(key => key.contains("a_service/a_method")))

    // use it to confirm metrics appear
    assert("hello" == await(configured(TestService.Query.Args("hello"))))
    eventually {
      assert(stats.counter("a_service", "a_method", "logical", "requests")() == 1)
    }

    server.close()
  }

  test("methodBuilder can be closed and opened again") {
    val service = new TestService.MethodPerEndpoint {
      def query(x: String): Future[String] = Future.value(x)
      def question(y: String): Future[String] = Future.value(y)
      def inquiry(z: String): Future[String] = Future.value(z)
    }

    val server =
      serverImpl.serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), service)
    val client = clientImpl.withLabel("a_label")
    val name = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
    val builder: MethodBuilder = client.methodBuilder(name)

    val spe1 = builder.servicePerEndpoint[TestService.ServicePerEndpoint]("query1").query
    assert(await(spe1(TestService.Query.Args("echo1"))) == "echo1")

    await(spe1.close())
    assert(spe1.close().isDefined)
    val spe2 = builder.servicePerEndpoint[TestService.ServicePerEndpoint]("query2").query
    assert(spe2.isAvailable)
    assert(await(spe2(TestService.Query.Args("echo2"))) == "echo2")

    await(spe2.close())
    server.close()
  }

  test("methodBuilder throws ServiceClosedException after closing") {
    val service = new TestService.MethodPerEndpoint {
      def query(x: String): Future[String] = Future.value(x)
      def question(y: String): Future[String] = Future.value(y)
      def inquiry(z: String): Future[String] = Future.value(z)
    }

    val server =
      serverImpl.serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), service)
    val client = clientImpl.withLabel("a_label")
    val name = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
    val builder: MethodBuilder = client.methodBuilder(name)

    val spe = builder.servicePerEndpoint[TestService.ServicePerEndpoint]("query1").query
    assert(await(spe(TestService.Query.Args("echo1"))) == "echo1")

    await(spe.close())
    assert(spe.close().isDefined)

    assert(!spe.isAvailable)
    intercept[ServiceClosedException] {
      assert(await(spe(TestService.Query.Args("echo2"))) == "echo2")
    }

    server.close()
  }

  test("methodBuilder supports eager loadbalancer connections") {
    val server = serverImpl.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new TestService.MethodPerEndpoint {
        def query(x: String): Future[String] = Future.value(x)
        def question(y: String): Future[String] = Future.value(y)
        def inquiry(z: String): Future[String] = Future.value(z)
      }
    )

    val sr = new InMemoryStatsReceiver
    val client = clientImpl
    // ensure we install an lb which supports eager conns
      .withLoadBalancer(com.twitter.finagle.loadbalancer.Balancers.aperture())
      .withStatsReceiver(sr)
      .withLabel("eager_clnt")
    val name = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))

    { // First check that eager connections can be disabled
      val builder: MethodBuilder = client
        .configured(
          com.twitter.finagle.loadbalancer.aperture.EagerConnections(false)).methodBuilder(name)

      val service: TestService.ServicePerEndpoint =
        builder.servicePerEndpoint[TestService.ServicePerEndpoint]("query1")
      // We shouldn't have any connections yet, and not even the gauge since nothing has been initialized.
      assert(sr.gauges.get(Seq("eager_clnt", "connections")) == None)

      service.asClosable.close()
    }

    { // Now check with eager connections
      val builder: MethodBuilder =
        client
          .configured(
            com.twitter.finagle.loadbalancer.aperture.EagerConnections(true)).methodBuilder(name)

      val service: TestService.ServicePerEndpoint =
        builder.servicePerEndpoint[TestService.ServicePerEndpoint]("query1")
      eventually {
        // With eager connections we should have at least 1 connection open even though we've sent no requests
        assert(sr.gauges(Seq("eager_clnt", "connections")).apply() >= 1f)
        assert(sr.counters(Seq("eager_clnt", "requests")) == 0l)
      }

      service.asClosable.close()
    }

    server.close()
  }

  private class ServiceClassCaptor
      extends Stack.Module1[Thrift.param.ServiceClass, ServiceFactory[Request, Response]] {

    @volatile var serviceClass: Option[Class[_]] = _

    def make(
      p1: Thrift.param.ServiceClass,
      next: ServiceFactory[Request, Response]
    ): ServiceFactory[Request, Response] = {
      serviceClass = p1.clazz
      next
    }

    def role: Stack.Role = Stack.Role("")
    def description: String = ""
  }

  test("captures service class via servicePerEndpoint") {
    val server = serverImpl.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new TestService.MethodPerEndpoint {
        def query(x: String): Future[String] = Future.value(x)
        def question(y: String): Future[String] = Future.value(y)
        def inquiry(z: String): Future[String] = Future.value(z)
      }
    )

    val captor = new ServiceClassCaptor
    val client = clientImpl
      .withStack(stack => stack.prepend(captor))

    val name = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
    val builder: MethodBuilder = client.methodBuilder(name)

    builder.servicePerEndpoint[TestService.ServicePerEndpoint]("query1").query

    assert(captor.serviceClass == Some(classOf[TestService.ServicePerEndpoint]))

    server.close()
  }

  test("captures service class via newServiceIface") {
    val server = serverImpl.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new TestService.MethodPerEndpoint {
        def query(x: String): Future[String] = Future.value(x)
        def question(y: String): Future[String] = Future.value(y)
        def inquiry(z: String): Future[String] = Future.value(z)
      }
    )

    val captor = new ServiceClassCaptor
    val client = clientImpl
      .withStack(stack => stack.prepend(captor))

    val name = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
    val builder: MethodBuilder = client.methodBuilder(name)

    builder.newServiceIface[TestService.ServiceIface]("query1").query

    assert(captor.serviceClass == Some(classOf[TestService.ServiceIface]))

    server.close()
  }

  test(".toString is helpful") {
    val service = new TestService.MethodPerEndpoint {
      def query(x: String): Future[String] = Future.value(x)
      def question(y: String): Future[String] = Future.value(y)
      def inquiry(z: String): Future[String] = Future.value(z)
    }
    val server =
      serverImpl.serveIface(new InetSocketAddress(InetAddress.getLoopbackAddress, 0), service)
    val client = clientImpl.withLabel("a_label")
    val name = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
    val builder: MethodBuilder = client.methodBuilder(name)
    val mbToString = builder.toString

    assert(mbToString.startsWith("MethodBuilder("))
    assert(mbToString.contains("dest=Set(Inet("))
    assert(mbToString.contains("stack=Node("))
    assert(mbToString.contains("params=Stack.Params.") || mbToString.contains("params=Iterable"))
    assert(mbToString.contains("config=Config("))
    server.close()
  }

  test("backup stats are scoped correctly") {
    val server = serverImpl.serveIface(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new TestService.MethodPerEndpoint {
        def query(x: String): Future[String] = Future.value(x)
        def question(y: String): Future[String] = Future.value(y)
        def inquiry(z: String): Future[String] = Future.value(z)
      }
    )
    val stats = new InMemoryStatsReceiver()
    val timer = new MockTimer

    val intermediateClient = clientImpl
      .withStatsReceiver(stats).withLabel("a_label")
      .configured(param.Timer(timer))
    val name = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))

    val mb = intermediateClient.methodBuilder(name).idempotent(maxExtraLoad = 1.percent)
    val spe = mb.servicePerEndpoint[TestService.ServicePerEndpoint]("query1").query
    Time.withCurrentTimeFrozen { tc =>
      await(spe(TestService.Query.Args("echo1"))) == "echo1"
      tc.advance(5.seconds)
      timer.tick()
      assert(
        stats.stat("a_label", "query1", "backups", "send_backup_after_ms")()
          == List(1.0)
      )
    }
    spe.close()
    server.close()
  }

  test("BackupRequestFilter is added to the Registry") {
    val registry = new SimpleRegistry()
    GlobalRegistry.withRegistry(registry) {
      val server = serverImpl.serveIface(
        new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
        new TestService.MethodPerEndpoint {
          def query(x: String): Future[String] = Future.value(x)
          def question(y: String): Future[String] = Future.value(y)
          def inquiry(z: String): Future[String] = Future.value(z)
        }
      )
      val stats = new InMemoryStatsReceiver()
      val timer = new MockTimer

      val intermediateClient = clientImpl
        .withStatsReceiver(stats).withLabel("a_label")
        .configured(param.Timer(timer))
      val name = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))

      val mb = intermediateClient.methodBuilder(name).idempotent(maxExtraLoad = 5.percent)
      val inquirySPE = mb.servicePerEndpoint[TestService.ServicePerEndpoint]("inquiry").inquiry

      // Reuse the method builder to ensure configs for BRF have changed
      val nonIdempotentMB = mb.nonIdempotent
      val questionSPE =
        nonIdempotentMB.servicePerEndpoint[TestService.ServicePerEndpoint]("question1").question

      // Reuse the method builder to ensure configs for BRF have changed
      val idempotentMB = nonIdempotentMB.idempotent(maxExtraLoad = 1.percent)
      val querySPE = idempotentMB.servicePerEndpoint[TestService.ServicePerEndpoint]("query1").query

      await(inquirySPE(TestService.Inquiry.Args("echo1"))) == "echo1"
      await(questionSPE(TestService.Question.Args("echo1"))) == "echo1"
      await(querySPE(TestService.Query.Args("echo1"))) == "echo1"
      def key(methodName: String, suffix: String): Seq[String] =
        Seq("client", "thriftmux", "a_label", Showable.show(name), "methods", methodName, suffix)

      def filteredRegistry: Set[Entry] =
        registry.filter { entry =>
          entry.key.head == "client" && entry.key.contains("BackupRequestFilter")
        }.toSet

      val brfEntries =
        Set(
          Entry(
            key("query1", "BackupRequestFilter"),
            "maxExtraLoad: Some(0.01), sendInterrupts: true"),
          Entry(
            key("inquiry", "BackupRequestFilter"),
            "maxExtraLoad: Some(0.05), sendInterrupts: true")
        )
      assert(filteredRegistry.size == 2)
      assert(filteredRegistry == brfEntries)
      inquirySPE.close()
      querySPE.close()
      questionSPE.close()
      server.close()
    }
  }
}
