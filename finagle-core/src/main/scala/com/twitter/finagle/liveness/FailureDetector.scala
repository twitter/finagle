package com.twitter.finagle.liveness

import com.twitter.app.GlobalFlag
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.{Status, Stack}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.util.parsers.{duration, list}
import com.twitter.util.{Duration, Future, Promise}
import java.util.logging.Logger

/**
 * Failure detectors attempt to gauge the liveness of a peer,
 * usually by sending ping messages and evaluating response
 * times.
 *
 * A peer marked `Busy` may be revived. A peer marked `Closed` may not.
 *
 * `onClose` is satisfied when `FailureDetector` marks a peer as closed.
 */
private[finagle] trait FailureDetector {
  def status: Status

  def onClose: Future[Unit]
}

/**
 * The null failure detector is the most conservative: it uses
 * no information, and always gauges the session to be
 * [[Status.Open]].
 */
private object NullFailureDetector extends FailureDetector {
  def status: Status = Status.Open
  val onClose: Future[Unit] = Future.never
}

/**
 * The mock failure detector is for testing, and invokes the passed in
 * `fn` to decide what `status` to return.
 */
private class MockFailureDetector(fn: () => Status) extends FailureDetector {
  private[this] val _onClose = new Promise[Unit]
  def onClose: Future[Unit] = _onClose
  def status: Status = {
    val mockStatus = fn()
    if (mockStatus == Status.Closed) _onClose.setDone()
    mockStatus
  }
}

/**
 * GlobalFlag to configure FailureDetection used only in the
 * absence of any app-specified config. This is the default
 * behavior.
 *
 * Value of:
 * "none": turn off threshold failure detector
 * "threshold:minPeriod:closeTimeout":
 *         use the specified configuration for failure detection
 */
object sessionFailureDetector
    extends GlobalFlag[String](
      "threshold:5.seconds:4.seconds",
      "The failure detector used to determine session liveness " +
        "[none|threshold:minPeriod:closeTimeout]"
    )

/**
 * Companion object capable of creating a FailureDetector based on parameterized config.
 */
object FailureDetector {

  /**
   * Base type used to identify and configure the [[FailureDetector]].
   */
  sealed trait Config

  /**
   * Default config type which tells the [[FailureDetector]] to extract
   * config values from the sessionFailureDetector GlobalFlag.
   */
  case object GlobalFlagConfig extends Config

  /**
   * Indicated to use the [[com.twitter.finagle.liveness.NullFailureDetector]]
   * when creating a new detector
   */
  case object NullConfig extends Config

  /**
   * Indicated to use the [[com.twitter.finagle.liveness.MockFailureDetector]]
   * when creating a new detector
   */
  case class MockConfig(fn: () => Status) extends Config

  /**
   * Indicated to use the [[com.twitter.finagle.liveness.ThresholdFailureDetector]]
   * configured with these values when creating a new detector.
   *
   * The default `windowSize` and `threshold` are chosen from examining a
   * representative ping distribution in a Twitter data center. With long tail
   * distribution, we want a reasonably large window size to capture long RTTs
   * in the history. A small threshold makes the detection sensitive to potential
   * failures. There can be a low rate of false positive, which is fine in most
   * production cases with cluster redundancy.
   *
   * `closeTimeout` allows a session to be closed when a ping response times out
   * after 4 seconds. This allows sessions to be reestablished when there may be
   * a networking issue, so that it can choose an alternative networking path instead.
   * The default 4 seconds is pretty conservative regarding normal ping RTT.
   */
  case class ThresholdConfig(minPeriod: Duration = 5.seconds, closeTimeout: Duration = 4.seconds)
      extends Config

  /**
   * Helper class for configuring a [[FailureDetector]] within a
   * [[com.twitter.finagle.Stackable]] client
   */
  case class Param(param: Config) {
    def mk(): (Param, Stack.Param[Param]) =
      (this, Param.param)
  }

  case object Param {
    implicit val param = Stack.Param(Param(GlobalFlagConfig))
  }

  private[this] val log = Logger.getLogger(getClass.getName)

  /**
   * Instantiate a new FailureDetector based on the config type
   */
  private[finagle] def apply(
    config: Config,
    ping: () => Future[Unit],
    statsReceiver: StatsReceiver
  ): FailureDetector = {
    config match {
      case NullConfig => NullFailureDetector

      case MockConfig(fn) => new MockFailureDetector(fn)

      case cfg: ThresholdConfig =>
        new ThresholdFailureDetector(
          ping,
          cfg.minPeriod,
          cfg.closeTimeout,
          statsReceiver = statsReceiver
        )

      case GlobalFlagConfig =>
        parseConfigFromFlags(ping, statsReceiver = statsReceiver)
    }
  }

  /**
   * Fallback behavior: parse the sessionFailureDetector global flag and
   * instantiate the proper config.
   */
  private def parseConfigFromFlags(
    ping: () => Future[Unit],
    nanoTime: () => Long = System.nanoTime _,
    statsReceiver: StatsReceiver = NullStatsReceiver
  ): FailureDetector = {
    sessionFailureDetector() match {
      case list("threshold", duration(min), duration(closeTimeout)) =>
        new ThresholdFailureDetector(
          ping,
          min,
          closeTimeout,
          nanoTime,
          statsReceiver
        )

      case list("threshold", duration(min)) =>
        new ThresholdFailureDetector(ping, min, nanoTime = nanoTime, statsReceiver = statsReceiver)

      case list("threshold") =>
        new ThresholdFailureDetector(ping, nanoTime = nanoTime, statsReceiver = statsReceiver)

      case list("none") =>
        NullFailureDetector

      case list(_*) =>
        log.warning(s"unknown failure detector ${sessionFailureDetector()} specified")
        NullFailureDetector
    }
  }
}
