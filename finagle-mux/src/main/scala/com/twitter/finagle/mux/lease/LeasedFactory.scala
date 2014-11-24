package com.twitter.finagle.mux.lease

import com.twitter.finagle._
import com.twitter.util.{Future, Time}

object LeasedFactory {
  val role = Stack.Role("Leased")

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module0[ServiceFactory[Req, Rep]] {
      val role = LeasedFactory.role
      val description = "Bridge gap between ServiceFactory.isAvailable and Acting.isActive"
      def make(next: ServiceFactory[Req, Rep]) = {
        val mk: () => Future[Service[Req, Rep] with Acting] = { () =>
          next() map {
            // the warning is OK, this should go away when we fix the abstraction
            case svc: Service[Req, Rep] with Acting => svc
            case _ => throw new IllegalArgumentException("You are only permitted to pass Acting Services!")
          }
        }
        new LeasedFactory(mk)
      }
    }
}

/**
 * LeasedFactory is used for bridging the gap between ServiceFactory#isAvailable
 * and Acting#isActive.  It will be removed after Service has a higher fidelity
 * way of doing State inspection.
 */
private[finagle] class LeasedFactory[Req, Rep](mk: () => Future[Service[Req, Rep] with Acting])
    extends ServiceFactory[Req, Rep] { self =>
  private[this] var current: List[Service[Req, Rep] with Acting] = Nil

  private[this] def newService(svc: Service[Req, Rep] with Acting) = {
    self.synchronized {
      current ::= svc
    }
    new ServiceProxy(svc) {
      override def close(deadline: Time) = {
        self.synchronized {
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

  override def isAvailable: Boolean = self.synchronized { current forall { svc =>
    // svc.isActive means that the service is alive, and ready to handle calls
    // !svc.isAvailable means that svc is dead.
    // Once the service is dead, we know we should close the old service and try again.
    // This means that the ServiceFactory will be available again, if we try to acquire
    // a new service.
    // This is a temporary fix until we start using the SF#status API.
    svc.isActive || !svc.isAvailable
  }}

  def close(deadline: Time): Future[Unit] = Future.Done
}
