package com.twitter.finagle.filter

import com.twitter.conversions.time._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.util.{DefaultLogger, Rng}
import com.twitter.finagle.{Failure, Service}
import com.twitter.finagle.service.FailedService
import com.twitter.util._
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import org.junit.runner.RunWith
import org.scalactic.Tolerance
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class NackAdmissionFilterTest extends FunSuite {
  import Tolerance._
  // NB: [[DefaultWindow]] and [[DefaultSuccessRateThreshold]] values are
  //     arbitrary.
  val DefaultWindow: Long = 100
  val DefaultSuccessRateThreshold: Double = 1D/2
  val DefaultTimeout: Duration = 2.seconds

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
            _window: Long = DefaultWindow,
            _successRateThreshold: Double = DefaultSuccessRateThreshold
           ) {
    val log: Logger = DefaultLogger
    val timer: MockTimer = new MockTimer
    val statsReceiver: InMemoryStatsReceiver = new InMemoryStatsReceiver()

    val svc: Service[Int, Int] = Service.mk[Int, Int](v => Future.value(v))
    val failingSvc: Service[Int, Int] = new FailedService(Failure.rejected("mock failure"))

    val filter: NackAdmissionFilter[Int, Int] = new NackAdmissionFilter[Int, Int](
      _window, _successRateThreshold, random, statsReceiver)

    def successfulResponse(): Unit = {
      assert(Await.result(filter(1, svc), DefaultTimeout) == 1)
    }

    def failedResponse(msg: String): Unit = {
      val thrown: Failure = intercept[Failure] {
        Await.result(filter(1, failingSvc), DefaultTimeout)
      }
      if (msg.nonEmpty) {
        assert(thrown.getMessage == msg)
      }
    }

    /**
     * Simulates not sending a request and therefore not receiving a
     * response. This does not change [[filter]]'s [[successLikelihoodEma]],
     * but it does check that the filter drops the request and creates a
     * [[NackAdmissionFilter.overloadFailure]]. If it passes, then we
     * know that the success rate is below the failure threshold.
     */
    def testDropsRequest(): Unit = {
      failedResponse("failed fast because service is overloaded")
    }

    /**
     * Simulates sending a request and receiving a "failure" response,
     * as if the cluster is overloaded. This decreases [[filter]]'s
     * [[successLikelihoodEma]] and checks that the filter does not drop the
     * request. If it passes, then we know that the success rate is above the
     * failure threshold.
     */
    def testGetFailedResponse(): Unit = {
      failedResponse("mock failure")
    }

    /**
     * Simulates sending a request and receiving a "failure" response. We
     * can use this to decrease [[filter]]'s [[successLikelihoodEma]] without
     * testing for a particular failure message.
     */
    def failRequestsWithoutTest(): Unit = {
      failedResponse("")
    }

    /**
     * Simulates sending a request and receiving a "success" response. We can
     * use this to increase [[filter]]'s [[successLikelihoodEma]] while testing
     * that the response is a success.
     */
    def testGetSuccessfulResponse(): Unit = {
      successfulResponse()
    }

    /**
     * Calculates the expected successLikelihood EMA value after a sequence of
     * consecutive failures.
     *
     * @param originalEma EMA value to start from.
     * @param numFailures Number of consecutive failures to simulate.
     * @param window Window size.
     * @param unit [[TimeUnit]] for window.
     * @return Expected value of EMA.
     */
    def expectedEmaAfterFailures(
        originalEma: Double,
        numFailures: Int,
        window: Long,
        unit: TimeUnit = TimeUnit.MILLISECONDS): Double = {
      require(originalEma >= 0, "originalEma must lie in the interval [0, 1]")
      require(originalEma <= 1, "originalEma must lie in the interval [0, 1]")
      require(window > 0, "window size must be positive")

      val w: Double = Math.exp(-1.0/window)
      Math.pow(w, numFailures) * originalEma
    }

    /**
     * Calculates the number of consecutive failures required for the success
     * rate to drop below the failure threshold.
     *
     * @param originalEma EMA value to start from.
     * @param successRateThreshold EMA multiplier.
     * @param window Window size.
     * @param unit [[TimeUnit]] for window.
     * @return Required number of failures.
     */
    def failuresToDropRequests(
        originalEma: Double,
        successRateThreshold: Double,
        window: Long,
        unit: TimeUnit = TimeUnit.MILLISECONDS): Int = {
      require(originalEma >= 0, "originalEma must lie in the interval [0, 1]")
      require(originalEma <= 1, "originalEma must lie in the interval [0, 1]")
      require(successRateThreshold > 0, "multiplier must lie in (0, 1)")
      require(successRateThreshold < 1, "multiplier must lie in (0, 1)")
      require(window > 0, "window size must be positive")

      val w: Double = Math.exp(-1.0/window)
      val multiplier: Double = 1D/successRateThreshold
      -(Math.log(multiplier * originalEma) / Math.log(w)).toInt
    }
  }

  test("increments successLikelihood EMA when successfully serving a request") {
    val ctx = new Ctx
    import ctx._

    testGetSuccessfulResponse()
    assert(filter.hasSentRequest)
    assert(filter.successLikelihoodEma.last == 1)
  }

  test("doesn't increment successLikelihood EMA when we get a failed response") {
    val ctx = new Ctx
    import ctx._

    testGetFailedResponse()
    assert(filter.hasSentRequest)
    assert(filter.successLikelihoodEma.last == 0)
  }

  test("doesn't drop requests prematurely") {
    /**
     * lowRng is a pessimistic Rng which always fails requests once the success
     * rate dips below the failure threshold.
     */
    val lowRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(lowRng)
    import ctx._

    testGetSuccessfulResponse()
    val numFailures = failuresToDropRequests(
      filter.successLikelihoodEma.last, DefaultSuccessRateThreshold, DefaultWindow)
    for (_ <- 0 until numFailures) { testGetFailedResponse() }

    val sentRequest = filter.hasSentRequest
    val successRate = filter.successLikelihoodEma.last
    val multiplier = 1D/DefaultSuccessRateThreshold
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

    testGetSuccessfulResponse()
    val numFailures = failuresToDropRequests(
      filter.successLikelihoodEma.last, DefaultSuccessRateThreshold, DefaultWindow)
    for (_ <- 0 to numFailures) { failRequestsWithoutTest() }

    val sentRequest = filter.hasSentRequest
    val successRate = filter.successLikelihoodEma.last
    val multiplier = 1D/DefaultSuccessRateThreshold
    assert(sentRequest)
    assert(0 < successRate && successRate < 1)
    assert(1 > multiplier * successRate)
    testDropsRequest()
  }

  test("doesn't drop requests after success rate improves past failure threshold") {
    /**
     * We change customRng so it will always drop or always send requests,
     * depending on whether we want the cluster to become unhealthy or recover.
     */
    val customRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(customRng)
    import ctx._

    // Initialize the EMA with a success...
    testGetSuccessfulResponse()
    val numFailures = failuresToDropRequests(
      filter.successLikelihoodEma.last, DefaultSuccessRateThreshold, DefaultWindow)
    // Let the cluster become unhealthy...
    for (_ <- 0 to numFailures) { failRequestsWithoutTest() }

    val sentRequest = filter.hasSentRequest
    val successRate = filter.successLikelihoodEma.last
    val multiplier = 1D/DefaultSuccessRateThreshold
    assert(sentRequest)
    assert(0 < successRate && successRate < 1)
    assert(1 > multiplier * successRate)
    // ... and now, since customRng is pessimistic, we always drop new requests.
    testDropsRequest()

    // Now make the Rng always send requests...
    customRng.doubleVal = 1
    // Let the cluster become healthy...
    // TODO: Write a method to determine the required number of successes here
    for (_ <- 0 to numFailures) { testGetSuccessfulResponse() }
    // Make the Rng pessimistic again...
    customRng.doubleVal = 0
    // ... but now the success rate is above the failure threshold, so new
    // requests are not dropped.
    testGetFailedResponse()
    testGetSuccessfulResponse()
  }

  test("statsReceiver increments fastFailures counter correctly") {
    val lowRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(lowRng)
    import ctx._

    // Explicitly, we successfully _send_ the first request, but return a
    // failed response. We then drop the next nine requests. This is why the
    // counter correctly counts 9 fast / hard failures, and not 10.
    for (_ <- 0 to 9) { failRequestsWithoutTest() }

    assert(statsReceiver.counter("dropped_requests")() == 9)
  }

  test("expected EMA equals empirical value after n consecutive failures") {
    val ctx = new Ctx()
    import ctx._

    val expectedEma: Double = expectedEmaAfterFailures(1.0, 10, DefaultWindow)
    // 1 success, so that the Ema starts at 1
    testGetSuccessfulResponse()
    // then 10 failures
    for (_ <- 0 to 9) { failRequestsWithoutTest() }
    val actualEma = filter.successLikelihoodEma.last
    assert(expectedEma === actualEma +- 0.001)
  }

  test("expected number of failures is sufficient to fail fast") {
    val lowRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(lowRng)
    import ctx._

    val numberOfFailures: Int = failuresToDropRequests(1.0, DefaultSuccessRateThreshold, DefaultWindow)
    testGetSuccessfulResponse()

    for (_ <- 0 to numberOfFailures) { failRequestsWithoutTest() }
    testDropsRequest()
  }

  test("expected number of failures is necessary to fail fast") {
    val lowRng: CustomRng = new CustomRng(0)
    val ctx = new Ctx(lowRng)
    import ctx._

    val numberOfFailures: Int = failuresToDropRequests(1.0, DefaultSuccessRateThreshold, DefaultWindow)
    testGetSuccessfulResponse()

    for (_ <- 0 until numberOfFailures) { failRequestsWithoutTest() }
    testGetSuccessfulResponse()
  }

  test("negative window value throws IllegalArgumentException") {
    val lowRng: CustomRng = new CustomRng(0)
    val _window: Long = -10
    val errMsg: String = s"requirement failed: window must be positive: ${_window}"
    val thrown: IllegalArgumentException = intercept[IllegalArgumentException] {
      new Ctx(lowRng, _window)
    }
    assert(thrown.getMessage == errMsg)
  }

  test("window value of zero throws IllegalArgumentException") {
    val lowRng: CustomRng = new CustomRng(0)
    val _window: Long = 0
    val errMsg: String = s"requirement failed: window must be positive: ${_window}"
    val thrown: IllegalArgumentException = intercept[IllegalArgumentException] {
      new Ctx(lowRng, _window)
    }
    assert(thrown.getMessage == errMsg)
  }

  test("negative successRateThreshold value throws IllegalArgumentException") {
    val lowRng: CustomRng = new CustomRng(0)
    val _successRateThreshold: Double = -5.5
    val errMsg: String = s"requirement failed: successRateThreshold must lie in (0, 1): ${_successRateThreshold}"
    val thrown: IllegalArgumentException = intercept[IllegalArgumentException] {
      new Ctx(lowRng, DefaultWindow, _successRateThreshold)
    }
    assert(thrown.getMessage == errMsg)
  }

  test("successRateThreshold value of 0 throws IllegalArgumentException") {
    val lowRng: CustomRng = new CustomRng(0)
    val _successRateThreshold: Double = 0
    val errMsg: String = s"requirement failed: successRateThreshold must lie in (0, 1): ${_successRateThreshold}"
    val thrown: IllegalArgumentException = intercept[IllegalArgumentException] {
      new Ctx(lowRng, DefaultWindow, _successRateThreshold)
    }
    assert(thrown.getMessage == errMsg)
  }

  test("successRateThreshold value of 1 throws IllegalArgumentException") {
    val lowRng: CustomRng = new CustomRng(0)
    val _successRateThreshold: Double = 1
    val errMsg: String = s"requirement failed: successRateThreshold must lie in (0, 1): ${_successRateThreshold}"
    val thrown: IllegalArgumentException = intercept[IllegalArgumentException] {
      new Ctx(lowRng, DefaultWindow, _successRateThreshold)
    }
    assert(thrown.getMessage == errMsg)
  }

  test("successRateThreshold value greater than 1 throws IllegalArgumentException") {
    val lowRng: CustomRng = new CustomRng(0)
    val _successRateThreshold: Double = 1.5
    val errMsg: String = s"requirement failed: successRateThreshold must lie in (0, 1): ${_successRateThreshold}"
    val thrown: IllegalArgumentException = intercept[IllegalArgumentException] {
      new Ctx(lowRng, DefaultWindow, _successRateThreshold)
    }
    assert(thrown.getMessage == errMsg)
  }
}