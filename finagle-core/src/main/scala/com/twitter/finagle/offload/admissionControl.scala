package com.twitter.finagle.offload

import com.twitter.conversions.DurationOps._
import com.twitter.app.{Flaggable, GlobalFlag}
import com.twitter.logging.Logger
import com.twitter.util.Duration
import java.util.Locale

object admissionControl
    extends GlobalFlag[OffloadACConfig](
      OffloadACConfig.Disabled,
      """Experimental flag for enabling OffloadPool based admission control. The 
    |basic idea is to use the pending work queue for the OffloadFilter as a
    |signal that the system is overloaded.
    |
    |Note: other admission control mechanisms should be disabled if this is used.
    |They can be disabled via the flags:
    |`com.twitter.server.filter.throttlingAdmissionControl=none`
    |`com.twitter.server.filter.cpuAdmissionControl=none`
    |CPU based admission control is disabled by default while throttlingAdmissionControl
    |is enabled by default.
    |
    |Flag values
    |'none': Offload based admission control is disabled.
    |'default': The default configuration which may change. Currently the same as 'none'.
    |'enabled': Offload based admission control is enabled with common parameters.
    |'maxQueueDelay': the window over which to monitor queue health in terms of a duration. Example: 50.milliseconds""".stripMargin
    )

/**
 * The flag parsing bits for the `admissionControl` flag.
 */
private[twitter] sealed abstract class OffloadACConfig private ()

private[twitter] object OffloadACConfig {
  private val log = Logger.get()

  implicit val flaggable: Flaggable[OffloadACConfig] = new Flaggable[OffloadACConfig] {
    def parse(s: String): OffloadACConfig = s.toLowerCase(Locale.US) match {
      case "none" | "default" => Disabled
      case "enabled" => DefaultEnabledParams
      case other => parseConfig(other)
    }

    // Expected format:
    // 'failurePercentile:maxQueueDelay'
    private[this] def parseConfig(lowercaseFlag: String): OffloadACConfig = {
      import com.twitter.finagle.util.parsers._
      lowercaseFlag match {
        case list(duration(maxQueueDelay)) =>
          DefaultEnabledParams.copy(maxQueueDelay = maxQueueDelay)
        case unknown =>
          log.error(s"Unparsable OffloadFilterAdmissionControl value: $unknown")
          Disabled
      }
    }
  }

  final case class Enabled(maxQueueDelay: Duration) extends OffloadACConfig()
  case object Disabled extends OffloadACConfig()

  // These parameters have been derived empirically with a little bit of intuition for garnish.
  // - maxQueueDelay: The acceptable delay for tasks in the worker pool before we start to reject work.
  val DefaultEnabledParams: Enabled = Enabled(maxQueueDelay = 20.milliseconds)
}
