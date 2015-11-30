package com.twitter.finagle.client

import com.twitter.finagle.factory.BindingFactory
import com.twitter.finagle.loadbalancer.LoadBalancerFactory
import com.twitter.finagle.stats.FinagleStatsReceiver
import com.twitter.finagle.util.{StackRegistry, Showable}
import com.twitter.finagle._
import com.twitter.util.{Future, Time}
import java.util.logging.Level

private[twitter] object ClientRegistry extends StackRegistry {

  private[this] val sr = FinagleStatsReceiver.scope("clientregistry")
  private[this] val clientRegistrySize = sr.addGauge("size") { size }
  private[this] val initialResolutionTime = sr.counter("initialresolution_ms")

  def registryName: String = "client"

  /**
   * Get a Future which is satisfied when the dest of every currently
   * registered client is no longer pending.
   * @note The destination of clients may later change to pending.
   * @note Clients are only registered after creation (i.e. calling `newClient` or
   *       `build` with ClientBuilder APIs).
   * @note Experimental feature which will eventually be solved by exposing Service
   *       availability as a Var.
   */
  def expAllRegisteredClientsResolved(): Future[Set[String]] = synchronized {
    val fs: Iterable[Future[String]] = registrants.map { case StackRegistry.Entry(_, _, params) =>
      val param.Label(name) = params[param.Label]
      val LoadBalancerFactory.Dest(va) = params[LoadBalancerFactory.Dest]
      val param.Logger(log) = params[param.Logger]

      val resolved = va.changes.filter(_ != Addr.Pending).toFuture()
      resolved.map { resolution =>
        // the full resolution can be rather verbose for large clusters,
        // so be stingy with our output
        log.fine(s"${name} params ${params}")
        if (log.isLoggable(Level.FINER)) {
          log.finer(s"${name} resolved to ${resolution}")
        } else {
          resolution match {
            case bound: Addr.Bound =>
              log.info(s"${name} resolved to Addr.Bound, current size=${bound.addrs.size}")
            case _ =>
              log.info(s"${name} resolved to ${resolution}")
          }
        }

        name
      }
    }

    val start = Time.now
    Future.collect(fs.toSeq).map(_.toSet).ensure {
      initialResolutionTime.incr((Time.now - start).inMilliseconds.toInt)
    }
  }
}

private[finagle] object RegistryEntryLifecycle {
  val role = Stack.Role("RegistryEntryLifecycle")
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] = new Stack.Module[ServiceFactory[Req, Rep]] {
    val role = RegistryEntryLifecycle.role

    val description: String = "Maintains the ClientRegistry for the stack"
    def parameters: Seq[Stack.Param[_]] = Seq(
      implicitly[Stack.Param[BindingFactory.Dest]]
    )

    def make(
      params: Stack.Params,
      next: Stack[ServiceFactory[Req, Rep]]
    ): Stack[ServiceFactory[Req, Rep]] = {
      val BindingFactory.Dest(dest) = params[BindingFactory.Dest]
      val BindingFactory.BaseDtab(baseDtab) = params[BindingFactory.BaseDtab]

      // for the benefit of ClientRegistry.expAllRegisteredClientsResolved
      // which waits for these to become non-Pending
      val va = dest match {
        case Name.Bound(va) => va
        case Name.Path(path) => Namer.resolve(baseDtab(), path)
      }

      val shown = Showable.show(dest)
      val registeredParams = params + LoadBalancerFactory.Dest(va)
      ClientRegistry.register(shown, next, registeredParams)

      CanStackFrom.fromFun[ServiceFactory[Req, Rep]].toStackable(role, { factory: ServiceFactory[Req, Rep] =>
        new ServiceFactoryProxy[Req, Rep](factory) {
          override def close(deadline: Time): Future[Unit] = {
            ClientRegistry.unregister(shown, next, params)
            self.close(deadline)
          }
        }
      }) +: next
    }
  }
}
