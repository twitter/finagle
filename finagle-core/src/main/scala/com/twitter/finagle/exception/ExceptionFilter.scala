package com.twitter.finagle.exception

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.Future
import com.twitter.finagle.tracing.Trace

/**
 * A filter that resides in both the client- and server-side finagle service stacks and
 * passes the Throwable that underlies Throws to an ExceptionReceiver.
 *
 * It resides in the service stack under all circumstances. If this feature is undesired, the
 * filter will send the exceptional values to a NullExceptionReceiver.
 */
class ExceptionFilter[Req, Rep](exceptionReceiver: ExceptionReceiver)
  extends SimpleFilter[Req, Rep]
{
  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    service(request) onFailure { e =>
      exceptionReceiver.receive(e)
      Trace.recordBinary("finagle.exception", e.toString)
    }
  }
}
