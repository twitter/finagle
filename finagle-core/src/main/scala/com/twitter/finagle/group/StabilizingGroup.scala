package com.twitter.finagle.group

import com.twitter.concurrent.{Offer, Broker}
import com.twitter.finagle.builder.Cluster
import com.twitter.finagle.Group
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Future, Time, Duration}
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
    pollSpan: Duration = Duration.fromMilliseconds(100)
  ): Group[T] = {
    new StabilizingGroup(underlying, pulse, grace, statsReceiver, pollSpan)
  }

  /**
   * A StabilizingGroup conservatively removes elements
   * from a Group depending on the `pulse`. More specifically,
   * removes are delayed until the group is in a healthy state
   * for at least `grace` period.
   *
   * @param underlying The source group to poll for group updates.
   * @param pollSpan The duration that must elapse in between polls
   *        to the underlying group.
   * @param pulse An offer for communicating group health.
   * @param grace The duration that must elapse before an element
   *        is removed from the group.
   */
  private[this] class StabilizingGroup[T](
    underlying: Group[T],
    pulse: Offer[State.Health],
    grace: Duration,
    statsReceiver: StatsReceiver,
    pollSpan: Duration
  ) extends Group[T] {
    @volatile private[this] var current = underlying.members
    private[this] val adds = new Broker[T]()
    private[this] val removes = new Broker[T]()
    private[this] val timer = DefaultTimer.twitter
    private[this] val limbo = statsReceiver.addGauge("limbo") { current.size - underlying.members.size }

    def members = current

    import State._
    /**
     * Exclusively maintains the elements in current
     * based on adds, removes, and health transitions.
     * Removes are delayed for grace period and each health
     * transition resets the grace period.
     */
    private[this] def loop(remq: Queue[(T, Time)], h: Health, nextPoll: Time): Future[Unit] = Offer.select(
      adds.recv map { elem =>
        current += elem
        // ensure the added elem is not queued for removal.
        val newq = remq filter { case(e, _) =>  e != elem }
        loop(newq, h, nextPoll)
      },
      removes.recv map { elem =>
        val until = Time.now + grace
        loop(remq enqueue (elem, until), h, nextPoll)
      },
      pulse map {
        // if our health transitions into healthy,
        // reset removal times foreach elem in remq.
        case newh if h == newh =>
          loop(remq, newh, nextPoll)
        case Healthy =>
          loop(remq map { case (elem, _) => (elem, Time.now + grace) }, Healthy, nextPoll)
        case newh =>
          loop(remq, newh, nextPoll)
      },
      Offer.timeout(nextPoll - Time.now)(timer) map { _ =>
        val snap = underlying.members
        for (add <- snap &~ current) adds ! add
        def isQueued(elem: T) = remq exists { case (e, _) => e == elem }
        for (rem <- current &~ snap if !isQueued(rem)) removes ! rem
        loop(remq, h, Time.now + pollSpan)
      },
      if (h != Healthy || remq.isEmpty) Offer.never
      else {
        val ((elem, until), nextq) = remq.dequeue
        Offer.timeout(until - Time.now)(timer) map { _ =>
          current -= elem
          loop(nextq, h, nextPoll)
        }
      }
    )
    loop(Queue(), Healthy, Time.now)
  }

}