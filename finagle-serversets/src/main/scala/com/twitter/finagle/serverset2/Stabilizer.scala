package com.twitter.finagle.serverset2

import com.twitter.conversions.time._
import com.twitter.finagle.Addr
import com.twitter.util.{Event, Witness, Var, Timer, Duration}
import java.net.SocketAddress
import java.util.concurrent.atomic.AtomicInteger

private[serverset2] object Stabilizer {
  // Create an event of epochs for the given duration.
  def epochs(period: Duration)(implicit timer: Timer): Event[Unit] = 
    new Event[Unit] {
      def register(w: Witness[Unit]) = {
        timer.schedule(period) {
          w.notify(())
        }
      }
    }

  private case class State(
    limbo: Set[SocketAddress],
    active: Set[SocketAddress],
    addr: Addr)

  private val emptyState = State(Set.empty, Set.empty, Addr.Pending)

  /**
   * Stabilize the address relative to the supplied source of epochs,
   * such that any removed socket address in an Addr.Bound set is
   * placed in a limbo state until at least one epoch has passed.
   *
   * In practice, the source of epochs must correlate with a failure
   * detection interval; we consider an address dead if it has not
   * been observed for at least one epoch, and no failures
   * (Addr.Failed) have been observed in the same interval.
   */
  def apply(va: Var[Addr], epoch: Event[Unit])
  : Var[Addr] = Var.async(Addr.Pending: Addr) { u =>
    // We construct an Event[State] representing states after
    // successive address observations. The state contains two sets
    // of addresses: the "active" set is the set of all observed
    // addresses in the current epoch; the "limbo" set is active set
    // at the turn of the last epoch.
    //
    // Whenever a failure is observed, the limbo set promoted by
    // adding it to the active set; thus we can guarantee that set
    // limbo++active contains all addresses seen in at least one
    // epoch's period without intermittent failure.
    //
    // Our state also maintains the last observed value of `va`.
    //
    // Thus we interpret the stabilized address to be
    // Addr.Bound(limbo++active) when these are nonempty; otherwise
    // the last observed address.

    val states: Event[State] = (va.changes select epoch).foldLeft(emptyState) {
      // Addr update
      case (st@State(limbo, active, last), Left(addr)) =>
        addr match {
          case Addr.Failed(_) => 
            State(Set.empty, active++limbo, addr)

          case Addr.Bound(bound) =>
            State(limbo, active++bound, addr)

          case addr => 
            // Any other address simply propagates the address while
            // leaving the active/limbo set unchanged. Both active
            // and limbo have to expire in order for this address to
            // propagate.
            st.copy(addr=addr)
        }
      
      // The epoch turned
      case (st@State(limbo, active, last), Right(())) =>
        last match {
          case Addr.Bound(bound) =>
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

    val addrs = states map { case State(limbo, active, last) =>
      val all = limbo++active
      if (all.nonEmpty) Addr.Bound(all)
      else last
    } sliding(2) collect {
      // Quench duplicate updates
      case Seq(a) => a
      case Seq(fst, snd) if fst != snd => snd
    }

    addrs.register(Witness(u))
  }
}
