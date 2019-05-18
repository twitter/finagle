package com.twitter.finagle.loadbalancer.aperture

import com.twitter.finagle._
import com.twitter.finagle.loadbalancer.{BalancerNode, NodeT}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}
import com.twitter.finagle.util.Ema
import com.twitter.util.{Closable, Duration, Future, Return, Throw, Time}

/**
 * LoadBand is an aperture controller targeting a load band. `lowLoad` and `highLoad` are
 * thresholds used to adjust the aperture. Whenever the capacity-adjusted,
 * exponentially smoothed, load is less than `lowLoad`, the aperture is shrunk by
 * one serving unit. When it exceeds `highLoad`, the aperture is opened by one serving
 * unit.
 *
 * The upshot is that `lowLoad` and `highLoad` define an acceptable
 * band of load for each serving unit.
 *
 * There are several goals that the LoadBand controller tries to achieve:
 *
 * 1. Distributed clients should be able to converge on a uniform aperture size if
 * they are offered the same amount of load. The tighter the high and low bands, the
 * less "wiggle" room distributed clients have to diverge aperture sizes.
 *
 * 2. Large changes or oscillations in the aperture window size are minimized in order to
 * avoid creating undue resource (e.g. sessions) churn. The `smoothWindow` allows to
 * dampen the rate of changes by rolling the offered load into an exponentially weighted
 * moving average.
 *
 * @note When using deterministic aperture, the aperture is not resized on the request
 *       path. Instead it changes with server set updates. Due to the mechanism with
 *       which this gets mixed in, this trait works as a no-op when [[Aperture.dapertureActive]]
 *       is true.
 */
private[loadbalancer] trait LoadBand[Req, Rep] extends BalancerNode[Req, Rep] with Closable {
  self: Aperture[Req, Rep] =>

  protected type Node <: LoadBandNode

  protected def statsReceiver: StatsReceiver

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

  // Must only be used from within the intrinsic lock, which is the monoTime object
  private[this] var total: Int = 0
  private[this] var offeredLoadEma: Double = 0L
  private[this] val monoTime = new Ema.Monotime
  private[this] val ema = new Ema(smoothWin.inNanoseconds)

  // As noted above, d-aperture balancers do not need these metrics.
  private[this] val sr =
    if (dapertureActive) NullStatsReceiver
    else statsReceiver.scope("loadband")
  private[this] val widenCounter = sr.counter("widen")
  private[this] val narrowCounter = sr.counter("narrow")

  private[this] val emaGauge = sr.addGauge("offered_load_ema") {
    monoTime.synchronized(offeredLoadEma).toFloat
  }

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
    val nextOfferedLoadEma = monoTime.synchronized {
      total += delta
      val next = ema.update(monoTime.nanos(), total)
      offeredLoadEma = next
      next
    }

    // Compute the capacity-adjusted average load and adjust the
    // aperture accordingly. We make only directional adjustments as
    // required, incrementing or decrementing the aperture by 1.
    //
    // Adjustments are somewhat racy: aperture and units may change
    // from underneath us. But this is not a big deal. If we
    // overshoot, the controller will self-correct quickly.
    val avgLoad = nextOfferedLoadEma / logicalAperture

    if (avgLoad >= highLoad && logicalAperture < maxUnits) {
      widen()
      widenCounter.incr()
    } else if (avgLoad <= lowLoad && logicalAperture > minUnits) {
      narrow()
      narrowCounter.incr()
    }
  }

  // This must be mixed in with another type that has a `close()` method due to the `super.close` call
  abstract override def close(deadline: Time): Future[Unit] = {
    emaGauge.remove()
    super.close(deadline)
  }

  protected trait LoadBandNode extends NodeT[Req, Rep] {
    abstract override def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
      if (dapertureActive) {
        super.apply(conn)
      } else {
        adjustTotalLoad(1)
        super.apply(conn).transform {
          case Return(svc) =>
            Future.value(new ServiceProxy(svc) {
              override def close(deadline: Time): Future[Unit] =
                super.close(deadline).ensure {
                  adjustTotalLoad(-1)
                }
            })

          case t @ Throw(_) =>
            adjustTotalLoad(-1)
            Future.const(t)
        }
      }
    }
  }
}
