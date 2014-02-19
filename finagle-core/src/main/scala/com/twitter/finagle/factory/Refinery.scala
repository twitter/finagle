package com.twitter.finagle.factory

import com.twitter.util.{Future, Time}
import com.twitter.finagle._
import java.net.SocketAddress

/**
 * Class Refinery routes service requests (i.e. it's a
 * ServiceFactory) based on the current refined name. Refinery caches
 * a default destination; subsequent destinations are instantiated on
 * a one-off basis.
 */
private[finagle] class Refinery[Req, Rep](
  dest: Name, 
  newFactory: Name => ServiceFactory[Req, Rep],
  ctx: DtabCtx = Dtab
) extends ServiceFactory[Req, Rep] {
  @volatile private[this] var base: Dtab = ctx.base
  @volatile private[this] var self: ServiceFactory[Req, Rep] =
    newFactory(base.refine(dest))

  private[this] def newBase(): Unit = synchronized {
    if (ctx.base eq base) return
    
    // Note that his can possibly disrupt outstanding requests when
    // the base closes. However, dynamically changing bases should
    // basically never happen, and if it does, only on startup.
    //
    // This is a temporary hack: once we do proper caching of dtabs,
    // we won't need to make handling "base" a special case.
    self.close()
    val newBase = ctx.base
    self = newFactory(newBase.refine(dest))
    base = newBase
  }

  def apply(conn: ClientConnection): Future[Service[Req, Rep]] = {
    if (ctx.base ne base)
      newBase()

    if (ctx() eq base) self(conn) else {
      val factory = newFactory(ctx.refine(dest))
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
