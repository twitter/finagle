package com.twitter.finagle.group

import com.twitter.concurrent.{Offer, Broker}
import com.twitter.finagle.builder.Cluster
import com.twitter.finagle.Group
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.exp.Var
import com.twitter.util.{Future, Time, Timer, Duration}
import scala.collection.immutable.Queue

object StabilizingGroup {
  object State extends Enumeration {
    type Health = Value
    val Healthy, Unhealthy, Unknown = Value
  }

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
    private[this] val newSet = new Broker[Set[T]]()
    private[this] val limbo = statsReceiver.addGauge("limbo") { 
      members.size - underlying.members.size
    }

    underlying.set observe { newSet ! _ }
    protected val _set = Var(underlying.members)

    import State._
    /**
     * Exclusively maintains the elements in current
     * based on adds, removes, and health transitions.
     * Removes are delayed for grace period and each health
     * transition resets the grace period.
     */
    private[this] def loop(remq: Queue[(T, Time)], h: Health): Future[Unit] = Offer.select(
      pulse map {
        // if our health transitions into healthy,
        // reset removal times foreach elem in remq.
        case newh if h == newh =>
          loop(remq, newh)
        case Healthy =>
          // Transitioned to healthy: push back
          val newTime = Time.now + grace
          val newq = remq map { case (elem, _) => (elem, newTime) }
          loop(newq, Healthy)
        case newh =>
          loop(remq, newh)
      },
      newSet.recv map { newSet =>
        val snap  = set()
        var q = remq

        // Remove pending removes that are present
        // in this update.
        q = q filter { case (e, _) => !(newSet contains e) }
        _set() ++= newSet &~ snap

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
          _set() -= elem
          loop(nextq, h)
        }
      }
    )

    loop(Queue.empty, Healthy)
  }
}