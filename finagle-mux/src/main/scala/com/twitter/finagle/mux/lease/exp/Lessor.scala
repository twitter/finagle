package com.twitter.finagle.mux.lease.exp

import com.twitter.finagle.Stack
import com.twitter.util.Duration

/*
 * All of these APIs are provisional, and may disappear suddenly.
 */

// TODO: should no longer be private[finagle] when the api is locked down
// TODO: should not just work with Duration, ie be generic
// TODO: Lessors should expose capabilities
// TODO: npending is specific to servers, and isn't meaningful for other types
// of lessees.  Either Lessee should be server-specific or npending should be removed/improved
/**
 * The lessee provides an interface that lets the lessor notify the lessee about
 * lease information, and lets the lessor query the lessee for draining info.
 */
private[finagle] trait Lessee {

  /**
   * The Lessee is given the lease for d, starting now.
   */
  def issue(d: Duration): Unit

  /**
   * The number of pending requests.  Useful for draining.
   */
  def npending(): Int
}

/**
 * The Lessor is the entity that gives leases.
 */
private[finagle] trait Lessor {

  /**
   * The lessor will notify all lessees that have been registered and have not
   * yet been unregistered when they have the lease.
   */
  def register(lessee: Lessee): Unit

  /**
   * After a lessee has been unregistered, it will no longer be notified that it
   * has the lease.
   */
  def unregister(lessee: Lessee): Unit

  /**
   * Lessees should call observe when they know the duration of their request.
   */
  def observe(d: Duration): Unit

  /**
   * Lessees should call observeArrival when a request arrives.
   */
  def observeArrival(): Unit
}

private[finagle] object Lessor {
  case class Param(lessor: Lessor)
  implicit object Param extends Stack.Param[Param] {
    val default = Param(ClockedDrainer.flagged)
  }

  val nil: Lessor = new Lessor {
    def register(lessee: Lessee): Unit = ()
    def unregister(lessee: Lessee): Unit = ()
    def observe(d: Duration): Unit = ()
    def observeArrival(): Unit = ()
  }
}
