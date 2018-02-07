package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.{BalancerNode, NodeT}
import com.twitter.util._

private[loadbalancer] trait Expiration[Req, Rep] extends BalancerNode[Req, Rep] {
  self: Aperture[Req, Rep] with Expiration[Req, Rep] =>

  protected type Node <: ExpiringNode

  /**
   * The maximum idle time for an endpoint which has fallen out
   * of the aperture window. Once the idle time is met, the node
   * is closed.
   */
  protected def endpointIdleTime: Duration

  /**
   * Creates a new periodic task which will try to expire nodes that are
   * outside of the aperture window. The interval that the task is run at
   * is <= the `endpointIdleTime`. This means that idle nodes can live for
   * at most `endpointIdleTime` * 2. We take the loss in precision for the
   * performance gain of not needing to schedule granular timer tasks for
   * each individual endpoint.
   */
  protected def newExpiryTask(timer: Timer): TimerTask =
    timer.schedule(endpointIdleTime / 2) {
      val vec = self.dist.vector
      val vecIndices = vec.indices.iterator
      val apertureIndices = self.dist.indices
      while (vecIndices.hasNext) {
        val i = vecIndices.next
        if (!apertureIndices.contains(i)) {
          vec(i).tryExpire()
        }
      }
    }

  // a counter which tracks the number of endpoints which have expired.
  private[this] val expiredCounter = self.statsReceiver.counter("expired")

  /**
   * A NodeT which closes the services acquired from `super` when it
   * goes idle. The following criteria must be met for the Node to be
   * considered idle:
   *
   * 1. No service has been acquired for at least `endpointIdleTime` duration.
   *
   * 2. There are no outstanding service acquisition requests.
   */
  protected trait ExpiringNode extends NodeT[Req, Rep] {

    /**
     * Attempts to expire the underlying resources that back this endpoint
     * iff it is considered idle.
     */
    def tryExpire(): Unit = {
      if (shouldExpire()) {
        resetIdleTime()
        expiredCounter.incr()
        factory.remake()
      }
    }

    // The lock used to guard the bookkeeping of idleness
    // (i.e. `outstanding` and `idleTime`)
    private[this] val lock = new Object
    private[this] var outstanding = 0
    private[this] var idleTime: Time = Time.Top

    private[this] def shouldExpire(): Boolean = lock.synchronized {
      outstanding == 0 && Time.now - idleTime >= endpointIdleTime
    }

    private[this] def resetIdleTime(): Unit = lock.synchronized {
      idleTime = Time.Top
    }

    private[this] def onRequest(): Unit = lock.synchronized {
      outstanding += 1
      if (outstanding == 1)
        idleTime = Time.Top
    }

    private[this] def onResponse(): Unit = lock.synchronized {
      outstanding -= 1
      if (outstanding == 0)
        idleTime = Time.now
    }

    abstract override def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
      onRequest()
      super.apply(conn).transform {
        case Return(svc) =>
          Future.value(new ServiceProxy(svc) {
            override def close(deadline: Time) =
              super.close(deadline).ensure {
                onResponse()
              }
          })

        case t @ Throw(_) =>
          onResponse()
          Future.const(t)
      }
    }
  }
}
