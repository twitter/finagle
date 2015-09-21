package com.twitter.finagle.group

import com.twitter.concurrent.{Offer, Broker}
import com.twitter.finagle.Group
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util._
import scala.collection.immutable.Queue

@deprecated("Use StabilizingAddr instead", "6.7.5")
object StabilizingGroup {
  object State extends Enumeration {
    type Health = Value
    // explicitly define intuitive ids as they
    // are exported for stats.
    val Healthy = Value(1)
    val Unknown = Value(0)
    val Unhealthy = Value(-1)
  }

  @deprecated("Use StabilizingAddr instead", "6.7.5")
  def apply[T](
    underlying: Group[T],
    pulse: Offer[State.Health],
    grace: Duration,
    statsReceiver: StatsReceiver = NullStatsReceiver,
    timer: Timer = DefaultTimer.twitter
  ): Group[T] = {
    new StabilizingGroup(underlying, pulse, grace, statsReceiver, timer)
  }

  /**
   * A StabilizingGroup conservatively removes elements
   * from a Group depending on the `pulse`. More specifically,
   * removes are delayed until the group is in a healthy state
   * for at least `grace` period.
   *
   * @param underlying The source group to poll for group updates.
   * @param pulse An offer for communicating group health.
   *        Invariant: The offer should communicate Health in FIFO order
   *        with respect to time.
   * @param grace The duration that must elapse before an element
   *        is removed from the group.
   */
  private[this] class StabilizingGroup[T](
    underlying: Group[T],
    pulse: Offer[State.Health],
    grace: Duration,
    statsReceiver: StatsReceiver,
    implicit val timer: Timer
  ) extends Group[T] {
    import State._

    private[this] val newSet = new Broker[Set[T]]()

    @volatile private[this] var healthStat = Healthy.id
    private[this] val health = statsReceiver.addGauge("health") { healthStat }

    private[this] val limbo = statsReceiver.addGauge("limbo") {
      members.size - underlying.members.size
    }

    protected[finagle] val set = Var(underlying.members)

    /**
     * Exclusively maintains the elements in current
     * based on adds, removes, and health transitions.
     * Removes are delayed for grace period and each health
     * transition resets the grace period.
     */
    private[this] def loop(remq: Queue[(T, Time)], h: Health): Future[Unit] = Offer.select(
      pulse map { newh =>
        healthStat = newh.id

        // if our health transitions into healthy,
        // reset removal times foreach elem in remq.
        newh match {
          case newh if h == newh =>
            loop(remq, newh)
          case Healthy =>
            // Transitioned to healthy: push back
            val newTime = Time.now + grace
            val newq = remq map { case (elem, _) => (elem, newTime) }
            loop(newq, Healthy)
          case newh =>
            loop(remq, newh)
        }
      },
      newSet.recv map { newSet =>
        val snap  = set()
        var q = remq

        // Remove pending removes that are present
        // in this update.
        q = q filter { case (e, _) => !(newSet contains e) }
        set() ++= newSet &~ snap

        def inQ(elem: T) = q exists { case (e, _) => e == elem }
        for (el <- snap &~ newSet if !inQ(el)) {
          val until = Time.now + grace
          q = q enqueue (el, until)
        }

        loop(q, h)
      },
      if (h != Healthy || remq.isEmpty) Offer.never
      else {
        val ((elem, until), nextq) = remq.dequeue
        Offer.timeout(until - Time.now) map { _ =>
          set() -= elem
          loop(nextq, h)
        }
      }
    )

    loop(Queue.empty, Healthy)

    underlying.set.changes.register(Witness({ set =>
      // We can synchronize here because we know loop
      // is eager, and doesn't itself synchronize.
      newSet !! set
    }))
  }
}
