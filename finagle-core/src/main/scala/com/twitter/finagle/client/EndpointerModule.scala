package com.twitter.finagle.client

import com.twitter.finagle.{Stack, ServiceFactory, param, Address}
import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.stack.Endpoint
import java.net.InetSocketAddress

/**
 * A module that hooks in a `ServiceFactory` that directly makes connections,
 * intended to be used as the last `ServiceFactory` in the stack, the
 * endpointer.
 *
 * @param extraParams the parameters that `fn` will also need
 * @param fn function that returns a ServiceFactory given params and a remote host
 */
class EndpointerModule[Req, Rep](
    extraParams: Seq[Stack.Param[_]],
    fn: (Stack.Params, InetSocketAddress) => ServiceFactory[Req, Rep])
  extends Stack.Module[ServiceFactory[Req, Rep]] {

  val role = Endpoint
  val description = "Send requests over the wire"
  val parameters = Seq(
    implicitly[Stack.Param[Transporter.EndpointAddr]],
    implicitly[Stack.Param[param.Stats]]
  ) ++ extraParams

  def make(prms: Stack.Params, next: Stack[ServiceFactory[Req, Rep]]) = {
    val Transporter.EndpointAddr(addr) = prms[Transporter.EndpointAddr]
    val factory = addr match {
      case com.twitter.finagle.exp.Address.ServiceFactory(sf: ServiceFactory[Req, Rep], _) => sf
      case Address.Inet(ia, _) => fn(prms, ia)
      case Address.Failed(e) => new FailingFactory[Req, Rep](e)
    }
    Stack.Leaf(this, factory)
  }
}
