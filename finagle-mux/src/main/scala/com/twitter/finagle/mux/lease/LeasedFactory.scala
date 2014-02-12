package com.twitter.finagle.mux.lease

import com.twitter.finagle.{ClientConnection, Service, ServiceFactory, ServiceProxy}
import com.twitter.util.{Future, Time}

/**
 * LeasedFactory is used for bridging the gap between ServiceFactory#isAvailable
 * and Acting#isActive.  It will be removed after Service has a higher fidelity
 * way of doing State inspection.
 */
private[finagle] class LeasedFactory[Req, Rep](mk: () => Future[Service[Req, Rep] with Acting])
    extends ServiceFactory[Req, Rep] {
  private[this] var current: List[Acting] = Nil

  private[this] def newService(svc: Service[Req, Rep] with Acting) = {
    synchronized {
      current ::= svc
    }
    new ServiceProxy(svc) {
      override def close(deadline: Time) = {
        synchronized {
          current = current.filterNot(_ == svc)
        }
        super.close(deadline)
      }
    }
  }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] =
    mk() map { dispatcher =>
      newService(dispatcher)
    }

  override def isAvailable: Boolean = current forall (_.isActive)

  def close(deadline: Time): Future[Unit] = Future.Done
}
