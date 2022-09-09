package com.twitter.finagle.loadbalancer

import com.twitter.finagle._
import com.twitter.util.Throw
import com.twitter.util.Time
import com.twitter.util.Future
import com.twitter.util.Return
import java.util.concurrent.atomic.AtomicInteger

/**
 * Provides a Node whose 'load' is the current number of pending
 * requests and thus will result in least-loaded load balancer.
 * Pending requests are only decremented when a response is
 * satisfied, and thus, this load metric implicitly takes latency
 * into account.
 */
private trait LeastLoaded[Req, Rep] extends BalancerNode[Req, Rep] { self: Balancer[Req, Rep] =>

  protected type Node <: LeastLoadedNode

  protected trait LeastLoadedNode extends NodeT[Req, Rep] {

    private[this] val counter = new AtomicInteger(0)

    def load: Double = counter.get

    abstract override def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
      counter.incrementAndGet()
      super.apply(conn).transform {
        case Return(svc) =>
          Future.value(new ServiceProxy(svc) {
            override def close(deadline: Time): Future[Unit] =
              super.close(deadline).ensure {
                counter.decrementAndGet()
              }
          })

        case t @ Throw(_) =>
          counter.decrementAndGet()
          Future.const(t)
      }
    }
  }
}
