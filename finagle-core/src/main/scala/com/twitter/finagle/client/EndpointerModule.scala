package com.twitter.finagle.client

import com.twitter.finagle.service.FailingFactory
import com.twitter.finagle.stack.Endpoint
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Address, ServiceFactory, Stack, param}
import java.net.SocketAddress

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
  fn: (Stack.Params, SocketAddress) => ServiceFactory[Req, Rep])
    extends Stack.Module[ServiceFactory[Req, Rep]] {

  val role = Endpoint
  val description = "Send requests over the wire"
  val parameters = Seq(
    implicitly[Stack.Param[Transporter.EndpointAddr]],
    implicitly[Stack.Param[param.Stats]],
    implicitly[Stack.Param[Transport.ClientSsl]]
  ) ++ extraParams

  def make(prms: Stack.Params, next: Stack[ServiceFactory[Req, Rep]]) = {
    val Transporter.EndpointAddr(addr) = prms[Transporter.EndpointAddr]
    val factory = addr match {
      case Address.ServiceFactory(sf: ServiceFactory[Req, Rep], _) => sf
      case Address.Inet(ia, _) => fn(prms, ia)
      case Address.Failed(e) => new FailingFactory[Req, Rep](e)
    }
    Stack.leaf(this, factory)
  }
}
