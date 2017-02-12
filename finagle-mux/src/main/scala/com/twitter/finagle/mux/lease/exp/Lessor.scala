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
private[twitter] trait Lessee {
  /**
   * The Lessee is given the lease for d, starting now.
   */
  def issue(d: Duration)

  /**
   * The number of pending requests.  Useful for draining.
   */
  def npending(): Int
}

/**
 * The Lessor is the entity that gives leases.
 */
private[twitter] trait Lessor {

  /**
   * The lessor will notify all lessees that have been registered and have not
   * yet been unregistered when they have the lease.
   */
  def register(lessee: Lessee)

  /**
   * After a lessee has been unregistered, it will no longer be notified that it
   * has the lease.
   */
  def unregister(lessee: Lessee)

  /**
   * Lessees should call observe when they know the duration of their request.
   */
  def observe(d: Duration)

  /**
   * Lessees should call observeArrival when a request arrives.
   */
  def observeArrival()
}

private[twitter] object Lessor {
  case class Param(lessor: Lessor)
  implicit object Param extends Stack.Param[Param] {
    val default = Param(ClockedDrainer.flagged)
  }

  val nil: Lessor = new Lessor {
    def register(lessee: Lessee) = ()
    def unregister(lessee: Lessee) = ()
    def observe(d: Duration) = ()
    def observeArrival() = ()
  }
}
