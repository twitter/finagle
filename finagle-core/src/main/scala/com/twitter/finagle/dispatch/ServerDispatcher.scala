package com.twitter.finagle.dispatch

import com.twitter.finagle.Service
import com.twitter.finagle.transport.Transport
import com.twitter.util.Return

/**
 * A {{ServerDispatcher}} dispatches requests onto services from a
 * source such as a {{Transport}}.
 */
trait ServerDispatcher {
  /**
   * Accept no more requests.
   */
  def drain()
}

object ServerDispatcher {
  val nil: ServerDispatcher = new ServerDispatcher {
    def drain() {}
  }
}

/**
 * Dispatch requests from transport one at a time, queueing
 * concurrent requests.
 *
 * Transport errors are considered fatal; the service will be
 * released after any error.
 */
class SerialServerDispatcher[Req, Rep](trans: Transport[Rep, Req], service: Service[Req, Rep])
  extends ServerDispatcher
{
  @volatile private[this] var draining = false
  @volatile private[this] var reading = true

  private[this] def loop(): Unit = {
    reading = true
    trans.read() ensure {
      reading = false
    } flatMap { req =>
      service(req)
    } flatMap { rep =>
      trans.write(rep)
    } respond {
      case Return(()) if !draining =>
        loop()

      case _ =>
        trans.close()
        service.release()
    }
  }

  loop()

  // Note: this is racy, but that's inherent in draining (without
  // protocol support). Presumably, half-closing TCP connection is
  // also possible.
  def drain() {
    if (reading)
      trans.close()
    draining = true
  }
}
