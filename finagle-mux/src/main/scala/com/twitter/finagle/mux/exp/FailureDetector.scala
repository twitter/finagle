package com.twitter.finagle.mux.exp

import com.twitter.app.GlobalFlag
import com.twitter.conversions.time._
import com.twitter.finagle.Stack
import com.twitter.finagle.mux._
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.util.parsers.{double, duration, int, list}
import com.twitter.util.{Duration, Future}
import java.util.logging.Logger

/**
 * GlobalFlag to configure FailureDetection used only in the
 * absence of any app-specified config. This is the default (legacy)
 * behavior.
 * We can't name this 'failureDetector' because it won't
 * build on the Mac, due to its case-insensitive file system.
 */
object sessionFailureDetector extends GlobalFlag(
  "none",
  "The failure detector used to determine session liveness " +
      "[none|threshold:minPeriod:threshold:win:closeThreshold]") {
  override val name = "com.twitter.finagle.mux.sessionFailureDetector"
}

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
  // instance for Java access
  val globalFlagConfig = GlobalFlagConfig

  /**
   * Indicated to use the [[com.twitter.finagle.mux.NullFailureDetector]] when creating a new detector
   */
  case object NullConfig extends Config
  // instance for Java access
  val nullConfig = NullConfig

  /**
   * Indicated to use the [[com.twitter.finagle.mux.ThresholdFailureDetector]]
   * configured with these values when creating a new detector
   */
  case class ThresholdConfig(
      minPeriod: Duration = 100.milliseconds,
      threshold: Double = 2,
      windowSize: Int = 5,
      closeThreshold: Int = -1)
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
    // by default, tell the builder to parse the GlobalFlag value
    // (legacy behavior) By default the flag is 'none'
    implicit val param = Stack.Param(Param(GlobalFlagConfig))
  }

  private[this] val log = Logger.getLogger(getClass.getName)

  /**
   * Instantiate a new FailureDetector based on the config type
   */
  def apply(
    config: Config,
    ping: () => Future[Unit],
    close: () => Future[Unit],
    statsReceiver: StatsReceiver
  ): FailureDetector = {
    config match {
      case NullConfig => NullFailureDetector

      case cfg: ThresholdConfig =>
        new ThresholdFailureDetector(ping, close, cfg.minPeriod, cfg.threshold,
          cfg.windowSize, cfg.closeThreshold, statsReceiver = statsReceiver)

      case GlobalFlagConfig =>
        parseConfigFromFlags(ping, close, statsReceiver = statsReceiver)
    }
  }

  /**
   * Fallback behavior: parse the sessionFailureDetector global flag and
   * instantiate the proper config.
   */
  private def parseConfigFromFlags(
    ping: () => Future[Unit],
    close: () => Future[Unit],
    nanoTime: () => Long = System.nanoTime,
    statsReceiver: StatsReceiver = NullStatsReceiver
  ): FailureDetector = {
    sessionFailureDetector() match {
      case list("threshold", duration(min), double(threshold), int(win), int(closeThreshold)) =>
        new ThresholdFailureDetector(
          ping, close, min, threshold, win, closeThreshold, nanoTime, statsReceiver)

      case list("threshold", duration(min), double(threshold), int(win)) =>
        new ThresholdFailureDetector(ping, close, min, threshold, win, nanoTime = nanoTime, statsReceiver = statsReceiver)

      case list("threshold", duration(min), double(threshold)) =>
        new ThresholdFailureDetector(ping, close, min, threshold, nanoTime = nanoTime, statsReceiver = statsReceiver)

      case list("threshold", duration(min)) =>
        new ThresholdFailureDetector(ping, close, min, nanoTime = nanoTime, statsReceiver = statsReceiver)

      case list("threshold") =>
        new ThresholdFailureDetector(ping, close, nanoTime = nanoTime, statsReceiver = statsReceiver)

      case list("none") =>
        NullFailureDetector

      case list(_*) =>
        log.warning(s"unknown failure detector ${sessionFailureDetector()} specified")
        NullFailureDetector
    }
  }
}

