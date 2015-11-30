package com.twitter.finagle.serverset2

import com.twitter.concurrent.NamedPoolThreadFactory
import com.twitter.conversions.time._
import com.twitter.finagle.util.{HashedWheelTimer, TimerStats}
import com.twitter.finagle.stats.FinagleStatsReceiver
import com.twitter.finagle.{Addr, WeightedSocketAddress}
import com.twitter.util._
import java.net.SocketAddress
import java.util.concurrent.TimeUnit
import org.jboss.netty.{util => netty}

/**
 * An Epoch is a Event that notifies its listener
 * once per `period`
 */
private [serverset2] object Epoch {
  def apply(period: Duration)(implicit timer: Timer): Epoch =
    new Epoch(new Event[Unit] {
      override def register(w: Witness[Unit]): Closable =
        timer.schedule(period) {
          w.notify(())
        }
    }, period)
}

private[serverset2] class Epoch(
  val event: Event[Unit],
  val period: Duration
)

private[serverset2] object Stabilizer {

  // Use our own timer to avoid doing work in the global timer's thread and causing timer deviation
  // the notify() run each epoch can trigger some slow work.
  // nettyHwt required to get TimerStats
  private val nettyHwt = new netty.HashedWheelTimer(
      new NamedPoolThreadFactory("finagle-serversets Stabilizer timer", true/*daemons*/),
      HashedWheelTimer.TickDuration.inMilliseconds,
      TimeUnit.MILLISECONDS,
      HashedWheelTimer.TicksPerWheel)
  private val epochTimer = HashedWheelTimer(nettyHwt)

  TimerStats.deviation(
    nettyHwt,
    10.milliseconds,
    FinagleStatsReceiver.scope("zk2").scope("timer"))

  TimerStats.hashedWheelTimerInternals(
    nettyHwt,
    () => 10.seconds,
    FinagleStatsReceiver.scope("zk2").scope("timer"))

  private val notifyMs = FinagleStatsReceiver.scope("serverset2").scope("stabilizer").stat("notify_ms")

  // Create an event of epochs for the given duration.
  def epochs(period: Duration): Epoch =
    new Epoch(
      new Event[Unit] {
        def register(w: Witness[Unit]) = {
          epochTimer.schedule(period) {
            val elapsed = Stopwatch.start()
            w.notify(())
            notifyMs.add(elapsed().inMilliseconds)
          }
        }
      },
      period
    )


  // Used for delaying removals
  private case class State(
    limbo: Set[SocketAddress],
    active: Set[SocketAddress],
    addr: Addr)

  // Used for batching updates
  private case class States(
    publish: Option[Addr],
    last: Addr,
    next: Option[Addr],
    lastEmit: Time)

  private val emptyState = State(Set.empty, Set.empty, Addr.Pending)

  /**
   * Stabilize the address relative to the supplied source of removalEpochs,
   * such that any removed socket address in an Addr.Bound set is
   * placed in a limbo state until at least one removalEpoch has passed.
   * Also batches all updates (adds and removes) to trigger at most once
   * per batchEpoch.
   *
   * In practice, the source of removalEpochs must correlate with a failure
   * detection interval; we consider an address dead if it has not
   * been observed for at least one removalEpoch, and no failures
   * (Addr.Failed) have been observed in the same interval.
   *
   * All changes, added and removed socket addresses, are batched by
   * one update per batchEpoch, delaying adds by at most one batchEpoch.
   */
  def apply(va: Var[Addr], removalEpoch: Epoch, batchEpoch: Epoch)
  : Var[Addr] = Var.async(Addr.Pending: Addr) { u =>
    // We construct an Event[State] representing states after
    // successive address observations. The state contains two sets
    // of addresses: the "active" set is the set of all observed
    // addresses in the current removalEpoch; the "limbo" set is active set
    // at the turn of the last removalEpoch.
    //
    // Whenever a failure is observed, the limbo set promoted by
    // adding it to the active set; thus we can guarantee that set
    // limbo++active contains all addresses seen in at least one
    // removalEpoch's period without intermittent failure.
    //
    // Our state also maintains the last observed value of `va`.
    //
    // Thus we interpret the stabilized address to be
    // Addr.Bound(limbo++active) when these are nonempty; otherwise
    // the last observed address.
    //
    // The updates to this stabilized address are then batched and
    // triggered at most once per batchEpoch.

    val states: Event[State] = (va.changes select removalEpoch.event).foldLeft(emptyState) {
      // Addr update
      case (st@State(limbo, active, last), Left(addr)) =>
        addr match {
          case Addr.Failed(_) =>
            State(Set.empty, active++limbo, addr)

          case Addr.Bound(bound, _) =>
            State(limbo, merge(active, bound), addr)

          case addr =>
            // Any other address simply propagates the address while
            // leaving the active/limbo set unchanged. Both active
            // and limbo have to expire in order for this address to
            // propagate.
            st.copy(addr=addr)
        }

      // The removalEpoch turned
      case (st@State(limbo, active, last), Right(())) =>
        last match {
          case Addr.Bound(bound, _) =>
            State(active, bound, last)

          case Addr.Neg =>
            State(active, Set.empty, Addr.Neg)

          case Addr.Pending | Addr.Failed(_) =>
            // If the last address is nonbound, we ignore it and
            // maintain our state; we cannot demote the active set
            // when nonbound, since that would eventually promote
            // the address
            st
        }
    }

    val addrs = states.map { case State(limbo, active, last) =>
      val all = merge(limbo, active)
      if (all.nonEmpty) Addr.Bound(all)
      else last
    }

    // Trigger at most one change to state per batchEpoch
    val init = States(Some(Addr.Pending), Addr.Pending, None, Time.Zero)
    val batchedUpdates =
      (addrs select batchEpoch.event).foldLeft(init) { case (st, ev) =>
        val now = Time.now
        ev match {
          case Left(newAddr) =>
            // There's a change to the serverset but it's not different. Noop
            if (newAddr == st.last)
              st.copy(publish = None)
            // There's a change to the serverset, but we have published in < batchEpoch. Hold change.
            else if (now - st.lastEmit < batchEpoch.period)
              st.copy(publish = None, next = Some(newAddr))
            // There's a change to the serverset and we haven't published in >= batchEpoch. Publish.
            else
              States(Some(newAddr), newAddr, None, now)

          case Right(_) =>
            st.next match {
              // Epoch turned, but we have published in < batchEpoch. Noop
              case _ if now - st.lastEmit < batchEpoch.period =>
                st.copy(publish = None)
              // Epoch turned but there is no next state. Noop
              case None =>
                st.copy(publish = None)
              // Epoch turned, there's a next state, and we haven't published in >= batchEpoch. Publish.
              case Some(next) =>
                States(Some(next), next, None, now)
            }
        }
      }.collect {
        case States(Some(publish), _, _, _) => publish
      }

    batchedUpdates.register(Witness(u))
  }

  /**
   * Merge WeightedSocketAddresses with same underlying SocketAddress
   * preferring weights from `next` over `prev`.
   */
  private def merge(prev: Set[SocketAddress], next: Set[SocketAddress]): Set[SocketAddress] = {
    val nextStripped = next.map(WeightedSocketAddress.extract(_)._1)

    val legacy = prev.filter { addr =>
      val (sa, _) = WeightedSocketAddress.extract(addr)
      !nextStripped.contains(sa)
    }

    legacy ++ next
  }
}
