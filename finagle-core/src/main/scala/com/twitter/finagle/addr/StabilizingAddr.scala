package com.twitter.finagle.addr

import com.twitter.concurrent.{Offer, Broker}
import com.twitter.finagle.{Addr, Address}
import com.twitter.finagle.stats.{StatsReceiver, NullStatsReceiver}
import com.twitter.finagle.util.DefaultTimer
import com.twitter.util.{Duration, Future, Time, Timer}
import scala.collection.immutable.Queue

private[finagle] object StabilizingAddr {
  private[finagle /*(testing*/ ] object State extends Enumeration {
    type Health = Value
    // explicitly define intuitive ids as they
    // are exported for stats.
    val Healthy = Value(1)
    val Unknown = Value(0)
    val Unhealthy = Value(-1)
  }

  private def qcontains[T](q: Queue[(T, _)], elem: T): Boolean =
    q exists { case (e, _) => e == elem }

  /**
   * A StabilizingAddr conservatively removes elements from a bound
   * Addr depending on the source health (exposed through `pulse`).
   * More specifically, removes are delayed until the source is in a
   * healthy state for at least `grace` period.
   *
   * @param addr An offer for underlying address updates.
   * @param pulse An offer for communicating group health.
   *        Invariant: The offer should communicate Health in FIFO order
   *        with respect to time.
   * @param grace The duration that must elapse before an element
   *        is removed from the group.
   */
  def apply(
    addr: Offer[Addr],
    pulse: Offer[State.Health],
    grace: Duration,
    statsReceiver: StatsReceiver = NullStatsReceiver,
    timer: Timer = DefaultTimer
  ): Offer[Addr] = new Offer[Addr] {
    import State._

    implicit val injectTimer = timer

    @volatile var nq = 0
    @volatile var healthStat = Healthy.id

    val health = statsReceiver.addGauge("health") { healthStat }
    val limbo = statsReceiver.addGauge("limbo") { nq }
    val stabilized = new Broker[Addr]

    /**
     * Exclusively maintains the elements in current
     * based on adds, removes, and health transitions.
     * Removes are delayed for grace period and each health
     * transition resets the grace period.
     */
    def loop(
      remq: Queue[(Address, Time)],
      h: Health,
      active: Set[Address],
      needPush: Boolean,
      srcAddr: Addr
    ): Future[Unit] = {
      nq = remq.size
      Offer.select(
        pulse map { newh =>
          healthStat = newh.id

          // If our health transitions into healthy, reset removal
          // times foreach elem in remq.
          newh match {
            case newh if h == newh =>
              loop(remq, newh, active, needPush, srcAddr)
            case Healthy =>
              // Transitioned to healthy: push back
              val newTime = Time.now + grace
              val newq = remq map { case (elem, _) => (elem, newTime) }
              loop(newq, Healthy, active, needPush, srcAddr)
            case newh =>
              loop(remq, newh, active, needPush, srcAddr)
          }
        },
        addr map {
          case addr @ Addr.Bound(newSet, _) =>
            // Update our pending queue so that newly added
            // entries aren't later removed.
            var q = remq filter { case (e, _) => !(newSet contains e) }

            // Add newly removed elements to the remove queue.
            val until = Time.now + grace
            for (el <- active &~ newSet if !qcontains(q, el))
              q = q.enqueue((el, until))

            loop(q, h, active ++ newSet, true, addr)

          case addr =>
            // A nonbound address will enqueue all active members
            // for removal, so that if we become bound again, we can
            // continue on merrily.
            val until = Time.now + grace
            val q = remq.enqueue(active.map(el => (el, until)))

            loop(q, h, active, true, addr)
        },
        if (h != Healthy || remq.isEmpty) Offer.never
        else {
          // Note: remq is ordered by 'until' time.
          val ((elem, until), nextq) = remq.dequeue
          Offer.timeout(until - Time.now) map { _ => loop(nextq, h, active - elem, true, srcAddr) }
        },
        if (!needPush) Offer.never
        else {
          // We always bind if active is nonempty. Otherwise we
          // pass through the current active address.
          val attrs = srcAddr match {
            case Addr.Bound(_, attrs) => attrs
            case _ => Addr.Metadata.empty
          }
          val addr =
            if (active.nonEmpty) Addr.Bound(active, attrs)
            else srcAddr
          stabilized.send(addr) map { _ => loop(remq, h, active, false, srcAddr) }
        }
      )
    }

    loop(Queue.empty, Healthy, Set.empty, false, Addr.Pending)

    // Defer to the underlying Offer.
    def prepare() = stabilized.recv.prepare()
  }
}
