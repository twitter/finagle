package com.twitter.finagle.filter

import com.twitter.conversions.time._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.util.{DefaultLogger, Rng}
import com.twitter.finagle.{Failure, Service}
import com.twitter.finagle.service.FailedService
import com.twitter.util._
import java.util.logging.Logger
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NackAdmissionFilterTest extends FunSuite {
  // NB: [[DefaultWindow]] and [[DefaultNackRateThreshold]] values are
  //     arbitrary.
  val DefaultWindow: Duration = 3.seconds
  val DefaultNackRateThreshold: Double = 0.5
  val DefaultTimeout: Duration = 1.second

  class CustomRng(_doubleVal: Double) extends Rng {
    require(_doubleVal >= 0, "_doubleVal must lie in the interval [0, 1]")
    require(_doubleVal <= 1, "_doubleVal must lie in the interval [0, 1]")
    var doubleVal = _doubleVal
    def nextDouble() = doubleVal
    def nextInt() = 1
    def nextInt(n: Int) = 1
    def nextLong(n: Long) = 1
  }

  class Ctx(random: Rng = Rng.threadLocal,
            _window: Duration = DefaultWindow,
            _nackRateThreshold: Double = DefaultNackRateThreshold) {
    val log: Logger = DefaultLogger
    val timer: MockTimer = new MockTimer
    val statsReceiver: InMemoryStatsReceiver = new InMemoryStatsReceiver()

    val DefaultAcceptRateThreshold = 1.0 - DefaultNackRateThreshold

    val svc: Service[Int, Int] = Service.mk[Int, Int](v => Future.value(v))

    val nackFailure: Failure = Failure.rejected("mock nack")
    val interruptedFailure: Failure = new Failure("interrupted", None, Failure.Interrupted)
    val namingFailure: Failure = new Failure("naming", None, Failure.Naming)

    val nackingSvc: Service[Int, Int] = new FailedService(nackFailure)
    val failingInterruptedSvc: Service[Int, Int] = new FailedService(interruptedFailure)
    val failingNamingSvc: Service[Int, Int] = new FailedService(namingFailure)

    // Used to ensure that the accept rate is safely above or below the
    // accept rate threshold.
    val extraProbability: Double = 0.01

    val filter: NackAdmissionFilter[Int, Int] = new NackAdmissionFilter[Int, Int](
      _window, _nackRateThreshold, random, statsReceiver)

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
     * response. This does not change [[filter]]'s [[acceptProbability]],
     * but it does check that the filter drops the request and creates a
     * [[NackAdmissionFilter.overloadFailure]]. If it passes, then we
     * know that the nack rate is below the failure threshold.
     */
    def testDropsRequest(): Unit = {
      failedResponse("Failed fast because service is overloaded")
    }

    /**
     * Simulates sending a request and receiving a "nack" response, as if the
     * cluster is overloaded. This decreases [[filter]]'s
     * [[acceptProbability]] and checks that the filter does not drop the
     * request. If it passes, then we know that the nack rate is above the
     * failure threshold.
     */
    def testGetNack(): Unit = {
      failedResponse("mock nack")
    }

    /**
     * Simulates sending a request and receiving a "nack" response. We
     * can use this to decrease [[filter]]'s [[acceptProbability]] without
     * testing for a particular failure message.
     */
    def nackWithoutTest(): Unit = {
      failedResponse("")
    }

    /**
     * Simulates sending a request and receiving a successful response. We can
     * use this to increase [[filter]]'s [[acceptProbability]] while testing
     * that the response is a success.
     */
    def testGetSuccessfulResponse(): Unit = {
      successfulResponse()
    }
  }

  test("increases acceptProbability when request is not NACKed") {
    val ctx = new Ctx
    import ctx._

    // Decrease Ema to just below 1
    testGetNack()
    assert(filter.hasSentRequest)
    val firstEmaValue = filter.emaValue
    // Increment Ema
    testGetSuccessfulResponse()
    assert(filter.emaValue > firstEmaValue)
  }

  test("decreases acceptProbability when request is NACKed") {
    val ctx = new Ctx
    import ctx._

    testGetNack()
    assert(filter.hasSentRequest)
    assert(filter.emaValue < 1)
  }

  if (!sys.props.contains("SKIP_FLAKY"))
    test("doesn't drop requests prematurely") {
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
      testGetNack()
    }

    val sentRequest = filter.hasSentRequest
    val successRate = filter.emaValue
    val multiplier = 1D/DefaultAcceptRateThreshold
    assert(sentRequest)
    assert(0 < successRate && successRate < 1)
    assert(1 <= multiplier * successRate)
    // The next request should be served, not dropped
    testGetSuccessfulResponse()
  }

  test("drops requests in advance when overloaded") {
    val lowRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(lowRng)
    import ctx._

    // Make the server nack requests so that the EMA drops below the accept
    // rate threshold
    while (filter.emaValue >= DefaultAcceptRateThreshold) {
      nackWithoutTest()
    }

    val sentRequest = filter.hasSentRequest
    val successRate = filter.emaValue
    val multiplier = 1D/DefaultAcceptRateThreshold
    assert(sentRequest)
    assert(0 < successRate && successRate < 1)
    assert(1 > multiplier * successRate)
    testDropsRequest()
  }

  if (!sys.props.contains("SKIP_FLAKY"))
    test("doesn't drop requests after accept rate drops below threshold") {
    /**
     * We change customRng so it will always drop or always send requests,
     * depending on whether we want the cluster to become unhealthy or recover.
     */
    val customRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(customRng)
    import ctx._

    // Let the cluster become unhealthy...
    while (filter.emaValue >= DefaultAcceptRateThreshold) {
      nackWithoutTest()
    }

    val sentRequest = filter.hasSentRequest
    val successRate = filter.emaValue
    val multiplier = 1D/DefaultAcceptRateThreshold
    assert(sentRequest)
    assert(0 < successRate && successRate < 1)
    assert(1 > multiplier * successRate)
    // ... and now, since customRng is pessimistic, we always drop new requests.
    testDropsRequest()

    // Now make the Rng always send requests...
    customRng.doubleVal = 1
    // Let the cluster become healthy, with some buffer for an additional
    // rejection from the server...
    while (filter.emaValue < DefaultAcceptRateThreshold + extraProbability) {
      testGetSuccessfulResponse()
    }
    // Make the Rng pessimistic again...
    customRng.doubleVal = 0
    // ... but now the accept rate is above the threshold, so new requests
    // are not dropped, even after another rejection from the server.
    testGetNack()
    testGetSuccessfulResponse()
  }

  test("statsReceiver increments dropped_requests counter correctly") {
    val lowRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(lowRng, 100.milliseconds)
    import ctx._

    // Pass time so NACKs can significantly reduce the Ema value
    Time.sleep(200.milliseconds)
    // Explicitly, we successfully _send_ the first request, but return a
    // nack. We then drop the next nine requests. This is why the counter
    // correctly counts 9 fast / hard failures, and not 10.
    for (_ <- 0 to 9) { nackWithoutTest() }

    assert(statsReceiver.counter("dropped_requests")() == 9)
  }

  test("EMA below accept rate threshold is sufficient to fail fast") {
    val lowRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(lowRng)
    import ctx._

    // Make the server nack requests until the EMA drops below the accept
    // rate threshold.
    while (filter.emaValue >= DefaultAcceptRateThreshold) {
      nackWithoutTest()
    }
    testDropsRequest()
  }

  if (!sys.props.contains("SKIP_FLAKY"))
    test("EMA below accept rate threshold is necessary to fail fast") {
    val lowRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(lowRng)
    import ctx._

    // Make the server nack requests until the EMA is just below the accept
    // rate threshold.
    while (filter.emaValue < DefaultAcceptRateThreshold + extraProbability) {
      nackWithoutTest()
    }
    testGetSuccessfulResponse()
  }

  test("only accepts increase the EMA") {
    val lowRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(lowRng)
    import ctx._

    val originalEma = filter.emaValue

    failedResponse("interrupted", failingInterruptedSvc)
    assert(filter.emaValue == originalEma)
    failedResponse("naming", failingNamingSvc)
    assert(filter.emaValue == originalEma)

    testGetNack()
    assert(filter.emaValue < originalEma)
  }

  if (!sys.props.contains("SKIP_FLAKY"))
    test("EMA value is affected only by responses in rolling window") {
    val lowRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(lowRng, 100.milliseconds)
    import ctx._

    // EMA decreases
    for (_ <- 0 to 99) { nackWithoutTest() }
    // Wait for window to safely pass
    Time.sleep(101.milliseconds)
    // increase EMA
    testGetSuccessfulResponse()

    // EMA should be very close to 1
    assert(filter.emaValue > 1 - (5 * extraProbability))
  }

  test("EMA value cannot start at 0 if first request is NACKed") {
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
    val errMsg: String = s"requirement failed: window size must be positive: ${_window.inNanoseconds}"
    val thrown: IllegalArgumentException = intercept[IllegalArgumentException] {
      new Ctx(lowRng, _window)
    }
    assert(thrown.getMessage == errMsg)
  }

  test("window value of zero throws IllegalArgumentException") {
    val lowRng: CustomRng = new CustomRng(0)
    val _window: Duration = 0.seconds
    val errMsg: String = s"requirement failed: window size must be positive: ${_window.inNanoseconds}"
    val thrown: IllegalArgumentException = intercept[IllegalArgumentException] {
      new Ctx(lowRng, _window)
    }
    assert(thrown.getMessage == errMsg)
  }

  test("negative nackRateThreshold value throws IllegalArgumentException") {
    val lowRng: CustomRng = new CustomRng(0)
    val _nackRateThreshold: Double = -5.5
    val errMsg: String = s"requirement failed: nackRateThreshold must lie in (0, 1): ${_nackRateThreshold}"
    val thrown: IllegalArgumentException = intercept[IllegalArgumentException] {
      new Ctx(lowRng, DefaultWindow, _nackRateThreshold)
    }
    assert(thrown.getMessage == errMsg)
  }

  test("nackRateThreshold value of 0 throws IllegalArgumentException") {
    val lowRng: CustomRng = new CustomRng(0)
    val _nackRateThreshold: Double = 0
    val errMsg: String = s"requirement failed: nackRateThreshold must lie in (0, 1): ${_nackRateThreshold}"
    val thrown: IllegalArgumentException = intercept[IllegalArgumentException] {
      new Ctx(lowRng, DefaultWindow, _nackRateThreshold)
    }
    assert(thrown.getMessage == errMsg)
  }

  test("nackRateThreshold value of 1 throws IllegalArgumentException") {
    val lowRng: CustomRng = new CustomRng(0)
    val _nackRateThreshold: Double = 1
    val errMsg: String = s"requirement failed: nackRateThreshold must lie in (0, 1): ${_nackRateThreshold}"
    val thrown: IllegalArgumentException = intercept[IllegalArgumentException] {
      new Ctx(lowRng, DefaultWindow, _nackRateThreshold)
    }
    assert(thrown.getMessage == errMsg)
  }

  test("nackRateThreshold value greater than 1 throws IllegalArgumentException") {
    val lowRng: CustomRng = new CustomRng(0)
    val _nackRateThreshold: Double = 1.5
    val errMsg: String = s"requirement failed: nackRateThreshold must lie in (0, 1): ${_nackRateThreshold}"
    val thrown: IllegalArgumentException = intercept[IllegalArgumentException] {
      new Ctx(lowRng, DefaultWindow, _nackRateThreshold)
    }
    assert(thrown.getMessage == errMsg)
  }
}