package com.twitter.finagle.loadbalancer

import com.twitter.finagle.util.DefaultLogger
import com.twitter.util.{Activity, Closable, Future, Time}
import java.util.logging.Level
import scala.util.control.NonFatal

/**
 * A Balancer mix-in which provides the collection over which to load balance
 * by observing `endpoints`.
 */
private trait Updating[Req, Rep] extends Closable { self: Balancer[Req, Rep] =>

  /**
   * An activity representing the active set of EndpointFactories.
   */
  protected def endpoints: Activity[IndexedSeq[EndpointFactory[Req, Rep]]]

  /*
   * Subscribe to the Activity and dynamically update the load
   * balancer as it (successfully) changes.
   *
   * The observation is terminated when the Balancer is closed.
   */
  private[this] val observation = endpoints.states.respond {
    case Activity.Ok(newList) =>
      // We log here for completeness. Since this happens out-of-band
      // of requests, the failures are not easily exposed.
      try update(newList)
      catch {
        case NonFatal(exc) =>
          DefaultLogger.log(Level.WARNING, "Failed to update balancer", exc)
      }

    case Activity.Failed(exc) =>
      DefaultLogger.log(Level.WARNING, "Activity Failed", exc)

    case Activity.Pending => // nop
  }

  // This stub is required for correct operation of the Reasonable Scala compiler.
  // See https://github.com/twitter/rsc/issues/100 for details.
  private def super$close(self: Closable)(deadline: Time): Future[Unit] = ???

  // This must be mixed in with another type that has a `close()` method due to the `super.close` call
  abstract override def close(deadline: Time): Future[Unit] = {
    observation.close(deadline).before { super.close(deadline) }
  }
}
