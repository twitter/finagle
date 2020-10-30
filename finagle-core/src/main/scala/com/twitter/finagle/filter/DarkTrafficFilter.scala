package com.twitter.finagle.filter

import com.twitter.finagle._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.Annotation.BinaryAnnotation
import com.twitter.finagle.tracing.ForwardAnnotation
import com.twitter.finagle.util.Rng
import com.twitter.logging.{HasLogLevel, Level, Logger}
import com.twitter.util.Future

/**
 * Forwards dark traffic to the given service when the given function returns true for a request.
 *
 * @param darkService Service to take dark traffic
 * @param enableSampling if function returns true, the request will forward
 * @param statsReceiver keeps stats for requests forwarded, skipped and failed.
 * @param forwardAfterService forward the dark request after the service has processed the request
 *        instead of concurrently.
 */
class DarkTrafficFilter[Req, Rep](
  darkService: Service[Req, Rep],
  enableSampling: Req => Boolean,
  override val statsReceiver: StatsReceiver,
  forwardAfterService: Boolean)
    extends SimpleFilter[Req, Rep]
    with AbstractDarkTrafficFilter {

  import DarkTrafficFilter._

  def this(
    darkService: Service[Req, Rep],
    enableSampling: Req => Boolean,
    statsReceiver: StatsReceiver
  ) = this(darkService, enableSampling, statsReceiver, false)

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    // Set an identifier so we can later determine which two requests
    // match, as well as which request is the dark one.
    ForwardAnnotation.let(newKeyAnnotation()) {
      if (forwardAfterService) {
        service(request).ensure {
          sendDarkRequest(request)(enableSampling, darkService)
        }
      } else {
        serviceConcurrently(service, request)(enableSampling, darkService)
      }
    }
  }

  protected def handleFailedInvocation[R](request: R, t: Throwable): Unit = {
    val level = t match {
      case hll: HasLogLevel => hll.logLevel
      case _ => Level.WARNING
    }
    log.log(level, t, s"DarkTrafficFilter Failed invocation: ${t.getMessage}")
  }
}

object DarkTrafficFilter {
  val log: Logger = Logger.get("DarkTrafficFilter")

  val DarkRequestAnnotation = BinaryAnnotation("clnt/dark_request", true)
  def newKeyAnnotation() =
    BinaryAnnotation("clnt/dark_request_key", Rng.threadLocal.nextLong(Long.MaxValue))
}
