package com.twitter.finagle.loadbalancer

import com.twitter.finagle.{ClientConnection, Service, ServiceFactory, ServiceFactoryProxy, ServiceProxy}
import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.Rng
import com.twitter.util.{Throw, Time, Future, Return}
import java.util.concurrent.atomic.AtomicInteger

/**
 * Provide Nodes whose 'load' is the current number of pending
 * requests and thus will result in least-loaded load balancer.
 */
private trait LeastLoaded[Req, Rep] { self: Balancer[Req, Rep] =>
  protected def rng: Rng

  protected case class Node(
      factory: ServiceFactory[Req, Rep],
      counter: AtomicInteger,
      token: Int)
    extends ServiceFactoryProxy[Req, Rep](factory)
    with NodeT[Req, Rep] {

    type This = Node

    def load: Double = counter.get
    def pending: Int = counter.get

    override def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
      counter.incrementAndGet()
      super.apply(conn).transform {
        case Return(svc) =>
          Future.value(new ServiceProxy(svc) {
            override def close(deadline: Time) =
              super.close(deadline).ensure {
                counter.decrementAndGet()
              }
          })

        case t@Throw(_) =>
          counter.decrementAndGet()
          Future.const(t)
      }
    }
  }

  protected def newNode(factory: ServiceFactory[Req, Rep], statsReceiver: StatsReceiver) =
    Node(factory, new AtomicInteger(0), rng.nextInt())

  private[this] val failingLoad = new AtomicInteger(0)
  protected def failingNode(cause: Throwable) = Node(new FailingFactory(cause), failingLoad, 0)
}
