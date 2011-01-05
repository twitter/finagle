package com.twitter.finagle.service

import org.jboss.netty.util.Timer

import com.twitter.util.{Future, Promise, Duration, Throw}

import com.twitter.finagle.channel.TimedoutRequestException
import com.twitter.finagle.util.Conversions._

/**
 * A filter to apply a global timeout to the request. This allows,
 * e.g., for a server to apply a global request timeout.
 */
class TimeoutFilter[Req <: AnyRef, Rep <: AnyRef](
  timer: Timer, timeout: Duration)
  extends Filter[Req, Rep, Req, Rep]
{
  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val result = new Promise[Rep]

    // Dispatch to the service, but throw an exception if it takes too
    // long.
    val timeoutHandle =
      timer(timeout) { result.updateIfEmpty(Throw(new TimedoutRequestException)) }
    service(request) respond { response =>
      timeoutHandle.cancel()
      result.updateIfEmpty(response)
    }

    result
  }
}
