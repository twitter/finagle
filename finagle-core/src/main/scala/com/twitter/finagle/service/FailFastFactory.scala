package com.twitter.finagle.service

import java.net.ConnectException
import java.util.concurrent.atomic.AtomicInteger
import com.twitter.finagle.{FailFastException, WriteException, ServiceFactory, ServiceFactoryProxy, ClientConnection}
import com.twitter.util.Future


/**
 * ServiceFactory Proxy responsible for handling fast-fail behavior when hosts are unreachable
 */
class FailFastFactory[Req, Rep](
  underlying: ServiceFactory[Req, Rep],
  maxPending: Int = 1
) extends ServiceFactoryProxy(underlying) {

  // factory state
  @volatile private[service] var isLimited = false
  private[this] val pending = new AtomicInteger(0)

  /**
   * - If we are not limited: Just go through, every connection exception toggle the isLimited flag to true.
   * - If we are limited: Acquire a "Token" (AtomicInteger) that allow you to establish a connection. If the connection
   *   succeed the isLimited flag is set back to false.
   */
  override def apply(conn: ClientConnection) =
    if (isLimited) {
      val accept = pending.incrementAndGet() <= maxPending
      if (accept) {
        underlying(conn) onSuccess { _ =>
          isLimited = false
        } ensure {
          pending.decrementAndGet()
        }
      } else {
        pending.decrementAndGet()
        Future.exception(new FailFastException)
      }
    } else {
      underlying(conn) onFailure {
        case WriteException(_) => isLimited = true
        case _ => // Other exceptions don't change the factory state
      }
    }

  override def isAvailable =
    underlying.isAvailable && pending.get < maxPending

  override val toString = "fail_fast_%s".format(underlying.toString)
}
