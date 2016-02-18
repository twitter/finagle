package com.twitter.finagle.loadbalancer

import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.util.OnReady
import com.twitter.util.{Time, Activity, Future, Promise}

/**
 * A Balancer mix-in which provides the collection over which to load balance
 * by observing `activity`.
 */
private trait Updating[Req, Rep] extends Balancer[Req, Rep] with OnReady {
  private[this] val ready = new Promise[Unit]
  def onReady: Future[Unit] = ready

  /**
   * An activity representing the active set of ServiceFactories.
   */
  // Note: this is not a terribly good method name and should be
  // improved in a future commit.
  protected def activity: Activity[Traversable[ServiceFactory[Req, Rep]]]

  /*
   * Subscribe to the Activity and dynamically update the load
   * balancer as it (succesfully) changes.
   *
   * The observation is terminated when the Balancer is closed.
   */
  private[this] val observation = activity.states.respond {
    case Activity.Pending =>

    case Activity.Ok(newList) =>
      update(newList)
      ready.setDone()

    case Activity.Failed(_) =>
      // On resolution failure, consider the
      // load balancer ready (to serve errors).
      ready.setDone()
  }

  override def close(deadline: Time): Future[Unit] = {
    observation.close(deadline).transform { _ =>
      super.close(deadline)
    }.ensure {
      ready.setDone()
    }
  }
}
