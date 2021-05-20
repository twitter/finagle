package com.twitter.finagle.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.service.FailedService
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.tracing.Annotation.BinaryAnnotation
import com.twitter.finagle.tracing.{BufferingTracer, Record, Trace}
import com.twitter.finagle.util.{DefaultLogger, Ema, LossyEma, Rng}
import com.twitter.finagle.{Failure, FailureFlags, Service, ServiceFactory, Stack, param}
import com.twitter.util._
import java.util.logging.Logger
import org.scalatest.funsuite.AnyFunSuite

class NackAdmissionFilterTest extends AnyFunSuite {
  // NB: [[DefaultWindow]] and [[DefaultNackRateThreshold]] values are
  //     arbitrary.
  val DefaultWindow: Duration = 3.seconds
  val DefaultNackRateThreshold: Double = 0.5
  val DefaultTimeout: Duration = 1.second
  class CustomRng(_doubleVal: Double) extends Rng {
    require(_doubleVal >= 0, "_doubleVal must lie in the interval [0, 1]")
    require(_doubleVal <= 1, "_doubleVal must lie in the interval [0, 1]")
    var doubleVal: Double = _doubleVal
    def nextDouble(): Double = doubleVal
    def nextInt(): Int = 1
    def nextInt(n: Int): Int = 1
    def nextLong(n: Long): Long = 1
  }

  class Ctx(
    random: Rng = Rng(0x5eeded),
    _window: Duration = DefaultWindow,
    _nackRateThreshold: Double = DefaultNackRateThreshold) {
    val log: Logger = DefaultLogger
    val timer: MockTimer = new MockTimer
    val statsReceiver: InMemoryStatsReceiver = new InMemoryStatsReceiver()

    val DefaultAcceptRateThreshold: Double = 1.0 - DefaultNackRateThreshold

    val svc: Service[Int, Int] = Service.mk[Int, Int](v => Future.value(v))

    val nackFailure: Failure = Failure.rejected("mock nack")
    val interruptedFailure: Failure = new Failure("interrupted", None, FailureFlags.Interrupted)
    val namingFailure: Failure = new Failure("naming", None, FailureFlags.Naming)

    val nackingSvc: Service[Int, Int] = new FailedService(nackFailure)
    val failingInterruptedSvc: Service[Int, Int] = new FailedService(interruptedFailure)
    val failingNamingSvc: Service[Int, Int] = new FailedService(namingFailure)

    val rpsThreshold: Int = 5

    // Used to ensure that the accept rate is safely above or below the
    // accept rate threshold.
    val extraProbability: Double = 0.01

    val filter: NackAdmissionFilter[Int, Int] = new NackAdmissionFilter[Int, Int](
      _window,
      _nackRateThreshold,
      random,
      statsReceiver,
      Stopwatch.timeNanos
    )

    def successfulResponse(): Unit = {
      assert(Await.result(filter(1, svc), DefaultTimeout) == 1)
    }

    // Not all failures are nacks. This method allows us to respond with any
    // kind of Finagle Failure.
    def failedResponse(msg: String, svc: Service[Int, Int] = nackingSvc): Unit = {
      val thrown: Failure = intercept[Failure] {
        Await.result(filter(1, svc), DefaultTimeout)
      }
      if (msg.nonEmpty) {
        assert(thrown.getMessage == msg)
      }
    }

    /**
     * Simulates not sending a request and therefore not receiving a
     * response. This does not change [[filter]]'s acceptProbability,
     * but it does check that the filter drops the request and creates a
     * NackAdmissionFilter.overloadFailure. If it passes, then we
     * know that the nack rate is below the failure threshold.
     */
    def testDropsRequest(): Unit = {
      failedResponse("Request not issued to the backend due to observed overload.")
    }

    /**
     * Simulates sending a request and receiving a "nack" response, as if the
     * cluster is overloaded. This decreases [[filter]]'s
     * acceptProbability and checks that the filter does not drop the
     * request. If it passes, then we know that the nack rate is above the
     * failure threshold.
     */
    def testGetNack(): Unit = {
      failedResponse("mock nack")
    }

    /**
     * Simulates sending a request and receiving a "nack" response. We
     * can use this to decrease [[filter]]'s acceptProbability without
     * testing for a particular failure message.
     */
    def nackWithoutTest(): Unit = {
      failedResponse("")
    }

    /**
     * Simulates sending a request and receiving a successful response. We can
     * use this to increase [[filter]]'s acceptProbability while testing
     * that the response is a success.
     */
    def testGetSuccessfulResponse(): Unit = {
      successfulResponse()
    }
  }

  def testEnabled(desc: String)(f: TimeControl => Unit): Unit = {
    test(desc) {
      Time.withCurrentTimeFrozen { ctl => f(ctl) }
    }
  }

  testEnabled("Can be disabled by configuration param") { _ =>
    val nackSvc = ServiceFactory.const[Int, Int](new FailedService(Failure.rejected("goaway")))
    val stats = new InMemoryStatsReceiver

    val s: Stack[ServiceFactory[Int, Int]] =
      NackAdmissionFilter
        .module[Int, Int]
        .toStack(Stack.leaf(Stack.Role("svc"), nackSvc))

    val ps: Stack.Params = Stack.Params.empty + param.Stats(stats)

    val sf = s.make(ps + NackAdmissionFilter.Disabled)

    // Make sure the NAF module is skipped
    assert(sf.equals(nackSvc))
  }

  testEnabled("increases acceptProbability when request is not NACKed") { ctl =>
    val ctx = new Ctx
    import ctx._

    ctl.advance(10.milliseconds)
    // Decrease Ema to be below 1
    for (_ <- 1 to 2 * rpsThreshold) {
      testGetNack()
      // Need to update timestamp as well
      ctl.advance(10.milliseconds)
    }
    val firstEmaValue = filter.emaValue

    ctl.advance(10.milliseconds)

    // Increment Ema
    testGetSuccessfulResponse()
    assert(filter.emaValue > firstEmaValue)
  }

  testEnabled("decreases acceptProbability when request is NACKed") { ctl =>
    val ctx = new Ctx
    import ctx._
    ctl.advance(10.milliseconds)
    for (_ <- 1 to 2 * rpsThreshold) {
      testGetNack()
      ctl.advance(10.milliseconds)
    }
    assert(filter.emaValue < 1)
  }

  testEnabled("doesn't drop requests prematurely") { ctl =>
    /**
     * lowRng is a pessimistic Rng which always fails requests once the accept
     * rate drops below the threshold.
     */
    val lowRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(lowRng)
    import ctx._

    // Make the server reject requests just before the EMA drops below the
    // accept rate threshold
    while (filter.emaValue > DefaultAcceptRateThreshold + extraProbability) {
      ctl.advance(10.milliseconds)
      testGetNack()
    }

    val successRate = filter.emaValue
    val multiplier = 1d / DefaultAcceptRateThreshold

    assert(0 < successRate && successRate < 1)
    assert(1 <= multiplier * successRate)

    // The next request should be served, not dropped
    testGetSuccessfulResponse()
  }

  testEnabled("drops requests in advance when overloaded") { ctl =>
    val lowRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(lowRng)
    import ctx._

    // Make the server nack requests so that the EMA drops below the accept
    // rate threshold
    while (filter.emaValue > DefaultAcceptRateThreshold) {
      ctl.advance(10.milliseconds)
      nackWithoutTest()
    }

    val successRate = filter.emaValue
    val multiplier = 1d / DefaultAcceptRateThreshold

    assert(0 < successRate && successRate < 1)
    assert(1 > multiplier * successRate)
    testDropsRequest()
  }

  testEnabled("annotates dropped requests") { ctl =>
    val lowRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(lowRng)
    import ctx._
    val tracer = new BufferingTracer()
    Trace.letTracer(tracer) {

      while (filter.emaValue > DefaultAcceptRateThreshold) {
        ctl.advance(10.milliseconds)
        nackWithoutTest()
      }

      nackWithoutTest()
      val expected = Seq(
        (
          "clnt/NackAdmissionFilter_rejected",
          "probabilistically dropped because " +
            "nackRate 0.5000930677240232 over window 3.seconds exceeds nackRateThreshold 0.5"))
      val actual = tracer.iterator.toList collect {
        case Record(_, _, BinaryAnnotation(k, v), _) => k -> v
      }
      assert(expected == actual)
    }
  }

  testEnabled("doesn't drop requests after accept rate drops below threshold") { ctl =>
    /**
     * We change customRng so it will always drop or always send requests,
     * depending on whether we want the cluster to become unhealthy or recover.
     */
    val customRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(customRng)
    import ctx._

    // Let the cluster become unhealthy...
    while (filter.emaValue >= DefaultAcceptRateThreshold) {
      ctl.advance(10.milliseconds)
      nackWithoutTest()
    }

    val successRate = filter.emaValue
    val multiplier = 1d / DefaultAcceptRateThreshold

    assert(0 < successRate && successRate < 1)
    assert(1 > multiplier * successRate)
    // ... and now, since customRng is pessimistic, we always drop new requests.
    testDropsRequest()

    // Now make the Rng always send requests...
    customRng.doubleVal = 1
    // Let the cluster become healthy, with some buffer for an additional
    // rejection from the server...
    while (filter.emaValue < DefaultAcceptRateThreshold + extraProbability) {
      ctl.advance(10.milliseconds)
      testGetSuccessfulResponse()
    }
    // Make the Rng pessimistic again...
    customRng.doubleVal = 0
    // ... but now the accept rate is above the threshold, so new requests
    // are not dropped, even after another rejection from the server.
    testGetNack()
    testGetSuccessfulResponse()
  }

  testEnabled("Respects MaxDropProbability, and recovers.") { ctl =>
    val ctx = new Ctx(Rng.threadLocal)
    import NackAdmissionFilter.MaxDropProbability
    import ctx._

    val NumRequests = 1000

    // Nack until EMA is so low that it would not be likely for any requests to
    // get through without the MaxDropProbability.
    while (filter.emaValue > 10e-10) {
      ctl.advance(1.second)
      nackWithoutTest()
    }

    var dropCount = statsReceiver.counter("dropped_requests")()

    // Nack a bunch
    for (_ <- 0 to NumRequests) {
      ctl.advance(1.second)
      nackWithoutTest()
    }

    // This is a probability game. Let's test that we have allowed MaxDropProbability +/- 10%
    val maxDropped = NumRequests * (MaxDropProbability + 0.1)
    val minDropped = NumRequests * (MaxDropProbability - 0.1)

    // Subtract any requests dropped earlier
    dropCount = statsReceiver.counter("dropped_requests")() - dropCount

    assert(dropCount < maxDropped)
    assert(dropCount > minDropped)

    // Have a bunch of successful requests. Test recovery
    for (_ <- 0 to NumRequests) {
      ctl.advance(1.second)
      Await.result(filter(1, svc).unit.rescue { case _ => Future.Unit }, DefaultTimeout)
    }

    // Should be very high now.
    assert(filter.emaValue > 0.9)
    testGetSuccessfulResponse()
  }

  testEnabled("statsReceiver increments dropped_requests counter correctly") { ctl =>
    val lowRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(lowRng, 100.milliseconds)
    import ctx._

    ctl.advance(10.milliseconds)
    for (_ <- 1 to rpsThreshold) {
      testGetNack()
      ctl.advance(10.milliseconds)
    }

    // Pass time so NACKs can significantly reduce the Ema value
    ctl.advance(200.milliseconds)
    // Explicitly, we successfully _send_ the first request, but return a
    // nack. We then drop the next nine requests. This is why the counter
    // correctly counts 9 fast / hard failures, and not 10.
    for (_ <- 0 to 9) {
      ctl.advance(1.millisecond)
      nackWithoutTest()
    }

    assert(statsReceiver.counter("dropped_requests")() == 9)
  }

  testEnabled("EMA below accept rate threshold is sufficient to fail fast") { ctl =>
    val lowRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(lowRng)
    import ctx._

    // Make the server nack requests until the EMA drops below the accept
    // rate threshold.
    while (filter.emaValue >= DefaultAcceptRateThreshold) {
      ctl.advance(1.second)
      nackWithoutTest()
    }
    testDropsRequest()
  }

  testEnabled("EMA below accept rate threshold is necessary to fail fast") { ctl =>
    val lowRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(lowRng)
    import ctx._

    // Make the server nack requests until the EMA is just below the accept
    // rate threshold.
    while (filter.emaValue < DefaultAcceptRateThreshold + extraProbability) {
      ctl.advance(1.second)
      nackWithoutTest()
    }
    testGetSuccessfulResponse()
  }

  testEnabled("only accepts increase the EMA") { ctl =>
    val lowRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(lowRng)
    import ctx._

    val originalEma = filter.emaValue

    ctl.advance(10.milliseconds)
    failedResponse("interrupted", failingInterruptedSvc)
    assert(filter.emaValue == originalEma)

    ctl.advance(10.milliseconds)
    failedResponse("naming", failingNamingSvc)
    assert(filter.emaValue == originalEma)

    ctl.advance(10.milliseconds)
    for (_ <- 0 to rpsThreshold) {
      testGetNack()
      ctl.advance(10.milliseconds)
    }
    assert(filter.emaValue < originalEma)
  }

  testEnabled("EMA value cannot start at 0 if first request is NACKed") { _ =>
    val lowRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(lowRng, 100.milliseconds)
    import ctx._

    // First request is NACKed...
    nackWithoutTest()

    // ... but the Ema is > 0.
    assert(filter.emaValue > 0)
  }

  test("negative window value throws IllegalArgumentException") {
    val lowRng: CustomRng = new CustomRng(0)
    val _window: Duration = -10.seconds
    val errMsg: String = s"requirement failed: window size must be positive: ${_window}"
    val thrown: IllegalArgumentException = intercept[IllegalArgumentException] {
      new Ctx(lowRng, _window)
    }
    assert(thrown.getMessage == errMsg)
  }

  test("window value of zero throws IllegalArgumentException") {
    val lowRng: CustomRng = new CustomRng(0)
    val _window: Duration = 0.seconds
    val errMsg: String = s"requirement failed: window size must be positive: ${_window}"
    val thrown: IllegalArgumentException = intercept[IllegalArgumentException] {
      new Ctx(lowRng, _window)
    }
    assert(thrown.getMessage == errMsg)
  }

  test("negative nackRateThreshold value throws IllegalArgumentException") {
    val lowRng: CustomRng = new CustomRng(0)
    val _nackRateThreshold: Double = -5.5
    val errMsg: String =
      s"requirement failed: nackRateThreshold must lie in (0, 1): ${_nackRateThreshold}"
    val thrown: IllegalArgumentException = intercept[IllegalArgumentException] {
      new Ctx(lowRng, DefaultWindow, _nackRateThreshold)
    }
    assert(thrown.getMessage == errMsg)
  }

  test("nackRateThreshold value of 0 throws IllegalArgumentException") {
    val lowRng: CustomRng = new CustomRng(0)
    val _nackRateThreshold: Double = 0
    val errMsg: String =
      s"requirement failed: nackRateThreshold must lie in (0, 1): ${_nackRateThreshold}"
    val thrown: IllegalArgumentException = intercept[IllegalArgumentException] {
      new Ctx(lowRng, DefaultWindow, _nackRateThreshold)
    }
    assert(thrown.getMessage == errMsg)
  }

  test("nackRateThreshold value of 1 throws IllegalArgumentException") {
    val lowRng: CustomRng = new CustomRng(0)
    val _nackRateThreshold: Double = 1
    val errMsg: String =
      s"requirement failed: nackRateThreshold must lie in (0, 1): ${_nackRateThreshold}"
    val thrown: IllegalArgumentException = intercept[IllegalArgumentException] {
      new Ctx(lowRng, DefaultWindow, _nackRateThreshold)
    }
    assert(thrown.getMessage == errMsg)
  }

  test("nackRateThreshold value greater than 1 throws IllegalArgumentException") {
    val lowRng: CustomRng = new CustomRng(0)
    val _nackRateThreshold: Double = 1.5
    val errMsg: String =
      s"requirement failed: nackRateThreshold must lie in (0, 1): ${_nackRateThreshold}"
    val thrown: IllegalArgumentException = intercept[IllegalArgumentException] {
      new Ctx(lowRng, DefaultWindow, _nackRateThreshold)
    }
    assert(thrown.getMessage == errMsg)
  }

  testEnabled("can be triggered by failure-flag encoded nacks") { ctl =>
    class Reject extends FailureFlags[Reject] {
      val flags: Long = FailureFlags.Rejected

      protected def copyWithFlags(flags: Long): Reject = ???
    }

    val nack = new NackAdmissionFilter[Int, Int](
      DefaultWindow,
      DefaultNackRateThreshold,
      Rng(0x5eeded),
      NullStatsReceiver,
      Stopwatch.timeNanos
    )

    val filtered = nack.andThen(Service.mk[Int, Int](_ => Future.exception(new Reject)))
    while (nack.emaValue >= 1.0 - DefaultNackRateThreshold) {
      ctl.advance(1.second)
      Await.ready(filtered(100), 5.seconds)
    }
    val res = filtered(100)
    Await.ready(res, 5.seconds)

    res.poll.get.throwable match {
      case f: FailureFlags[_] => assert(f.isFlagged(FailureFlags.NonRetryable))
      case other => fail(s"expected a nack, got $other")
    }
  }

  test("LossyEma behaves the same as Ema") {
    Time.withTimeAt(Time.Zero) { tc =>
      val window = 2.minutes.inNanoseconds
      val now = Stopwatch.timeNanos
      val lossy = new LossyEma(window, now, 1.0)
      val ema = new Ema(window)
      ema.update(now(), 1.0)

      // loop, advancing the clock 10 ms per cycle to "poorly" imitate 100 rps.
      // doing it for 100 minutes.
      val delta = 10.milliseconds
      val rng = Rng(101010101L)
      val successRate = 0.9995
      0.until(100).foreach { min =>
        0.until(100).foreach { req =>
          tc.advance(delta)
          val input = if (rng.nextDouble() < successRate) 1.0 else 0.0
          lossy.update(input)
          ema.update(now(), input)
          assert(lossy.last == ema.last, s"minute=$min request=$req")
        }
      }
    }
  }
}
