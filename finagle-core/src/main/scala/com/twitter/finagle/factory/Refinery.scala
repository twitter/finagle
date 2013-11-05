package com.twitter.finagle.factory

import com.twitter.util.{Future, Time}
import com.twitter.finagle._
import java.net.SocketAddress

/**
 * Class Refinery routes service requests (i.e. it's a
 * ServiceFactory) based on the current refined name. Refinery caches
 * a default destination; subsequent destinations are instantiated on
 * a one-off basis.
 *
 * @todo Maintain a cache of non-default factories.
 */
private[finagle] class Refinery[Req, Rep](
  dest: Name, 
  newFactory: Name => ServiceFactory[Req, Rep],
  baseDtab: Dtab = Dtab.base
) extends ServiceFactory[Req, Rep] {
  private[this] val self = newFactory(baseDtab.refine(dest))

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    if (Dtab() == baseDtab) {
      self(conn)
    } else {
      // TODO: cache empty groups here.
      val factory = newFactory(Dtab.refine(dest))
      factory(conn) map { service =>
        new ServiceProxy(service) {
          override def close(deadline: Time) =
            super.close(deadline) transform { case _ =>
              factory.close(deadline) 
            }
        }
      }
    }
  }

  def close(deadline: Time) = self.close(deadline)
  override def isAvailable = self.isAvailable
}
