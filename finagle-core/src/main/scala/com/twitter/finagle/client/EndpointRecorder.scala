package com.twitter.finagle.client

import com.twitter.finagle._
import com.twitter.finagle.naming.BindingFactory
import com.twitter.finagle.param.Label
import com.twitter.util._

private[finagle] object EndpointRecorder {
  val role = Stack.Role("EndpointRecorder")

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[EndpointRecorder]].
   */
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.ModuleParams[ServiceFactory[Req, Rep]] {
      val role: Stack.Role = EndpointRecorder.role

      val description: String = "Records endpoints in the endpoint registry"

      val parameters = Seq(
        implicitly[Stack.Param[Label]],
        implicitly[Stack.Param[BindingFactory.BaseDtab]],
        implicitly[Stack.Param[BindingFactory.Dest]]
      )

      def make(params: Stack.Params, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        val BindingFactory.Dest(dest) = params[BindingFactory.Dest]
        dest match {
          case bound: Name.Bound =>
            val Label(client) = params[Label]
            val BindingFactory.BaseDtab(baseDtab) = params[BindingFactory.BaseDtab]
            new EndpointRecorder(
              next,
              EndpointRegistry.registry,
              client,
              baseDtab() ++ Dtab.local,
              bound.idStr,
              bound.addr
            )
          case _ => next
        }

      }
    }
}

/**
 * A [[com.twitter.finagle.ServiceFactoryProxy]] that passes endpoint information
 * to the [[EndpointRegistry]]. It manifests as a module below
 * [[BindingFactory]] so deregistration can happen when a connection is closed.
 *
 * @param registry Registry to register endpoints to
 * @param client Name of the client
 * @param dtab Dtab for this path
 * @param path Path of this serverset
 * @param endpoints collection of addrs for this serverset
 */
private[finagle] class EndpointRecorder[Req, Rep](
  underlying: ServiceFactory[Req, Rep],
  registry: EndpointRegistry,
  client: String,
  dtab: Dtab,
  path: String,
  endpoints: Var[Addr])
    extends ServiceFactoryProxy[Req, Rep](underlying) {

  registry.addObservation(client, dtab, path, endpoints)

  override def close(deadline: Time): Future[Unit] = {
    registry.removeObservation(client, dtab, path)
    underlying.close(deadline)
  }
}
