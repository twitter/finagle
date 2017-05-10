package com.twitter.finagle.loadbalancer

import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.util.DefaultLogger
import com.twitter.util.{Time, Activity, Future}
import java.util.logging.Level
import scala.util.control.NonFatal

/**
 * A Balancer mix-in which provides the collection over which to load balance
 * by observing `activity`.
 */
private trait Updating[Req, Rep] extends Balancer[Req, Rep] {
  /**
   * An activity representing the active set of ServiceFactories.
   */
  protected def endpoints: Activity[IndexedSeq[ServiceFactory[Req, Rep]]]

  /*
   * Subscribe to the Activity and dynamically update the load
   * balancer as it (succesfully) changes.
   *
   * The observation is terminated when the Balancer is closed.
   */
  private[this] val observation = endpoints.states.respond {
    case Activity.Ok(newList) =>
      // We log here for completeness. Since this happens out-of-band
      // of requests, the failures are not easily exposed.
      try update(newList) catch {
        case NonFatal(exc) =>
          DefaultLogger.log(Level.WARNING, "Failed to update balancer", exc)
      }

    case Activity.Failed(exc) =>
      DefaultLogger.log(Level.WARNING, "Activity Failed", exc)

    case Activity.Pending => // nop
  }

  override def close(deadline: Time): Future[Unit] = {
    observation.close(deadline).before { super.close(deadline) }
  }
}
