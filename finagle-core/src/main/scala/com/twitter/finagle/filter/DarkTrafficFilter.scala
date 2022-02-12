package com.twitter.finagle.filter

import com.twitter.finagle._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.tracing.Annotation.BinaryAnnotation
import com.twitter.finagle.tracing.ForwardAnnotation
import com.twitter.finagle.util.Rng
import com.twitter.logging.HasLogLevel
import com.twitter.logging.Level
import com.twitter.logging.Logger
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
    val invokeDarkTraffic = enableSampling(request)
    val isDarkRequestAnnotation = if (invokeDarkTraffic) DarkRequestTrue else DarkRequestFalse
    // Set an identifier so both light service and dark service can
    // query the annotation from tracing or from Finagle Local
    // context, the same request going through both services should
    // have the same dark request key.
    ForwardAnnotation.let(Seq(newKeyAnnotation(), isDarkRequestAnnotation)) {
      if (forwardAfterService) {
        service(request).ensure {
          sendDarkRequest(request)(invokeDarkTraffic, darkService)
        }
      } else {
        serviceConcurrently(service, request)(invokeDarkTraffic, darkService)
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

  // the presence of clnt/is_dark_request indicates that the span is associated with a dark request
  val DarkRequestAnnotation = BinaryAnnotation("clnt/is_dark_request", true)
  // the value of clnt/has_dark_request indicates whether or not the request contains a span that is forwarded to dark service
  def DarkRequestTrue = BinaryAnnotation("clnt/has_dark_request", true)
  def DarkRequestFalse = BinaryAnnotation("clnt/has_dark_request", false)
  def newKeyAnnotation() =
    BinaryAnnotation("clnt/dark_request_key", Rng.threadLocal.nextLong(Long.MaxValue))
}
