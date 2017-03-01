package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.{Balancer, NodeT}
import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.finagle.util.Ema
import com.twitter.util.{Duration, Future, Return, Time, Throw}
import java.util.concurrent.atomic.AtomicInteger

/**
 * LoadBand is an aperture controller targeting a load band.
 * `lowLoad` and `highLoad` are watermarks used to adjust the
 * aperture. Whenever the the capacity-adjusted, exponentially
 * smoothed, load is less than `lowLoad`, the aperture is shrunk by
 * one serving unit; when it exceeds `highLoad`, the aperture is
 * opened by one serving unit.
 *
 * The upshot is that `lowLoad` and `highLoad` define an acceptable
 * band of load for each serving unit.
 */
private[loadbalancer] trait LoadBand[Req, Rep] {
  self: Balancer[Req, Rep] with Aperture[Req, Rep] =>

  /**
   * The time-smoothing factor used to compute the capacity-adjusted
   * load. Exponential smoothing is used to absorb large spikes or
   * drops. A small value is typical, usually on the order of
   * seconds.
   */
  protected def smoothWin: Duration

  /**
   * The lower bound of the load band.
   * Must be less than [[highLoad]].
   */
  protected def lowLoad: Double

  /**
   * The upper bound of the load band.
   * Must be greater than [[lowLoad]].
   */
  protected def highLoad: Double

  private[this] val total = new AtomicInteger(0)
  private[this] val monoTime = new Ema.Monotime
  private[this] val ema = new Ema(smoothWin.inNanoseconds)

  /**
   * Adjust `node`'s load by `delta`.
   */
  private[this] def adjustNode(node: Node, delta: Int) = {
    node.counter.addAndGet(delta)

    // this is synchronized so that sampling the monotonic time and updating
    // based on that time are atomic, and we don't run into problems like:
    //
    // t1:
    // sample (ts = 1)
    // t2:
    // sample (ts = 2)
    // update (ts = 2)
    // t1:
    // update (ts = 1) // breaks monotonicity
    val avg = synchronized {
      ema.update(monoTime.nanos(), total.addAndGet(delta))
    }

    // Compute the capacity-adjusted average load and adjust the
    // aperture accordingly. We make only directional adjustments as
    // required, incrementing or decrementing the aperture by 1.
    //
    // Adjustments are somewhat racy: aperture and units may change
    // from underneath us. But this is not a big deal. If we
    // overshoot, the controller will self-correct quickly.
    val a = avg/aperture

    if (a >= highLoad && aperture < units)
      widen()
    else if (a <= lowLoad && aperture > minAperture)
      narrow()
  }

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
      adjustNode(this, 1)
      super.apply(conn).transform {
        case Return(svc) =>
          Future.value(new ServiceProxy(svc) {
            override def close(deadline: Time): Future[Unit] =
              super.close(deadline).ensure {
                adjustNode(Node.this, -1)
              }
          })

        case t@Throw(_) =>
          adjustNode(this, -1)
          Future.const(t)
      }
    }
  }

  protected def newNode(
    factory: ServiceFactory[Req, Rep],
    statsReceiver: StatsReceiver
  ): Node = Node(factory, new AtomicInteger(0), rng.nextInt())

  private[this] val failingLoad = new AtomicInteger(0)

  protected def failingNode(cause: Throwable): Node =
    Node(new FailingFactory(cause), failingLoad, 0)
}