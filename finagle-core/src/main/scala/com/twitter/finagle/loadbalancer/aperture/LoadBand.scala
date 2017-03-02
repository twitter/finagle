package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.Balancer
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
   * Adjust `total` by `delta` in order to keep track of total load across all
   * nodes.
   */
  private[this] def adjustTotalLoad(delta: Int): Unit = {
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

  override protected def newFactory(
    sf: ServiceFactory[Req, Rep]
  ): ServiceFactory[Req, Rep] =
    new ServiceFactoryProxy(sf) {
      override def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
        adjustTotalLoad(1)
        super.apply(conn).transform {
          case Return(svc) =>
            Future.value(new ServiceProxy(svc) {
              override def close(deadline: Time) =
                super.close(deadline).ensure {
                  adjustTotalLoad(-1)
                }
            })

          case t@Throw(_) =>
            adjustTotalLoad(-1)
            Future.const(t)
        }
      }
    }
}