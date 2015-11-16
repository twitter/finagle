package com.twitter.finagle.mux

import com.twitter.app.GlobalFlag
import com.twitter.conversions.time._
import com.twitter.finagle.{Status, Stack}
import com.twitter.finagle.mux._
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.util.parsers.{double, duration, int, list}
import com.twitter.util.{Duration, Future}
import java.util.logging.Logger

/**
 * Failure detectors attempt to gauge the liveness of a peer,
 * usually by sending ping messages and evaluating response
 * times.
 */
private[mux] trait FailureDetector {
  def status: Status
}

/**
 * The null failure detector is the most conservative: it uses
 * no information, and always gauges the session to be
 * [[Status.Open]].
 */
private object NullFailureDetector extends FailureDetector {
  def status: Status = Status.Open
}

/**
 * GlobalFlag to configure FailureDetection used only in the
 * absence of any app-specified config. This is the default
 * behavior.
 *
 * Value of:
 * "none": turn off threshold failure detector
 * "threshold:minPeriod:threshold:win:closeTimeout":
 *         use the specified configuration for failure detection
 */
object sessionFailureDetector extends GlobalFlag(
  "threshold:5.seconds:2:100:duration.top",
  "The failure detector used to determine session liveness " +
      "[none|threshold:minPeriod:threshold:win:closeTimeout]")


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
   * Indicated to use the [[com.twitter.finagle.mux.NullFailureDetector]]
   * when creating a new detector
   */
  case object NullConfig extends Config

  /**
   * Indicated to use the [[com.twitter.finagle.mux.ThresholdFailureDetector]]
   * configured with these values when creating a new detector.
   *
   * The default `windowSize` and `threshold` are chosen from examining a
   * representative ping distribution in a Twitter data center. With long tail
   * distribution, we want a reasonably large window size to capture long RTTs
   * in the history. A small threshold makes the detection sensitive to potential
   * failures. There can be a low rate of false positive, which is fine in most
   * production cases with cluster redundancy.
   */
  case class ThresholdConfig(
      minPeriod: Duration = 5.seconds,
      threshold: Double = 2,
      windowSize: Int = 100,
      closeTimeout: Duration = Duration.Top)
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
  private[mux] def apply(
    config: Config,
    ping: () => Future[Unit],
    statsReceiver: StatsReceiver
  ): FailureDetector = {
    config match {
      case NullConfig => NullFailureDetector

      case cfg: ThresholdConfig =>
        new ThresholdFailureDetector(ping, cfg.minPeriod, cfg.threshold,
          cfg.windowSize, cfg.closeTimeout, statsReceiver = statsReceiver)

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
    nanoTime: () => Long = System.nanoTime,
    statsReceiver: StatsReceiver = NullStatsReceiver
  ): FailureDetector = {
    sessionFailureDetector() match {
      case list("threshold", duration(min), double(threshold), int(win), duration(closeTimeout)) =>
        new ThresholdFailureDetector(
          ping, min, threshold, win, closeTimeout, nanoTime, statsReceiver)

      case list("threshold", duration(min), double(threshold), int(win)) =>
        new ThresholdFailureDetector(
          ping, min, threshold, win, nanoTime = nanoTime, statsReceiver = statsReceiver)

      case list("threshold", duration(min), double(threshold)) =>
        new ThresholdFailureDetector(
          ping, min, threshold, nanoTime = nanoTime, statsReceiver = statsReceiver)

      case list("threshold", duration(min)) =>
        new ThresholdFailureDetector(
          ping, min, nanoTime = nanoTime, statsReceiver = statsReceiver)

      case list("threshold") =>
        new ThresholdFailureDetector(
          ping, nanoTime = nanoTime, statsReceiver = statsReceiver)

      case list("none") =>
        NullFailureDetector

      case list(_*) =>
        log.warning(s"unknown failure detector ${sessionFailureDetector()} specified")
        NullFailureDetector
    }
  }
}