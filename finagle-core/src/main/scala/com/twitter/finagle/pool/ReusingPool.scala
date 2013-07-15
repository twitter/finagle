package com.twitter.finagle.pool

import com.twitter.finagle.{ClientConnection, Service, ServiceFactory,
  ServiceFactoryProxy, ServiceProxy}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.util.{Future, Return, Throw, Time, Promise}
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec

/**
 * A pool that maintains at most one service from the underlying
 * ServiceFactory. A new Service is established whenever the service
 * factory fails or the current service is unavailable.
 */
class ReusingPool[Req, Rep](
  underlying: ServiceFactory[Req, Rep], 
  statsReceiver: StatsReceiver)
extends ServiceFactoryProxy[Req, Rep](underlying) {
  private[this] object stats {
    val conn = statsReceiver.scope("connects")
    val fail = conn.counter("fail")
    val dead = conn.counter("dead")
  }

  private[this] val current: AtomicReference[Future[ServiceProxy[Req, Rep]]] =
    new AtomicReference(Future.exception(new Exception))

  private[this] def newService(conn: ClientConnection) =
    underlying(conn) map { service =>
      new ServiceProxy(service) {
        override def close(deadline: Time) = Future.Done
      }
    }

  @tailrec
  override final def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    /*
     * Current contains either (1) a live service, which we can use;
     * (2) a pending connect request, which we must wait for; (3) a
     * failed connect which we must retry; or (4) a dead service
     * which triggers a reconnect.
     *
     * The reconnect behavior is entirely driven by the caller, and
     * so is the timeout -- this is arguably the correct behavior,
     * but could lead to some strange semantics: for example, any
     * interrupt will cause the connection to be interrupted. So if
     * there are multiple requests with different timeouts, the
     * effective timeout of the connect is the smallest of these.
     * Arguably we should wait for every request to issue a timeout
     * (requires a lot of bookeeping), or do our own timeout
     * management here (simpler and easier to understand).
     */
    val f = current.get()

    f.poll match {
      case Some(Return(service)) if service.isAvailable => f
      case None => f  // Still waiting for connect.
      case Some(Throw(_)) =>  // Connect failed; retry.
        stats.fail.incr()
        val p = new Promise[ServiceProxy[Req, Rep]]
        if (!current.compareAndSet(f, p)) apply(conn) else {
          p.become(newService(conn))
          p
        }
      case Some(Return(dead)) =>  // Service died; reconnect.
        stats.dead.incr()
        val p = new Promise[ServiceProxy[Req, Rep]]
        if (!current.compareAndSet(f, p)) apply(conn) else {
          dead.self.close()
          p.become(newService(conn))
          p
        }
    }
  }
}
