package com.twitter.finagle.client

import com.twitter.finagle.{Service, ServiceFactory, Stack, Stackable}
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.param.{Label, ProtocolLibrary}
import com.twitter.finagle.transport.{Transport, TransportContext}
import java.net.SocketAddress

trait StdStackClient[Req, Rep, This <: StdStackClient[Req, Rep, This]]
    extends EndpointerStackClient[Req, Rep, This] { self =>

  /**
   * The type we write into the transport.
   */
  protected type In

  /**
   * The type we read out of the transport.
   */
  protected type Out

  /**
   * The type of the transport's context.
   */
  protected type Context <: TransportContext

  /**
   * Defines a typed [[com.twitter.finagle.client.Transporter]] for this client.
   * Concrete StackClient implementations are expected to specify this.
   */
  protected def newTransporter(addr: SocketAddress): Transporter[In, Out, Context]

  /**
   * Defines a dispatcher, a function which reconciles the stream based
   * `Transport` with a Request/Response oriented `Service`.
   * Together with a `Transporter`, it forms the foundation of a
   * finagle client. Concrete implementations are expected to specify this.
   *
   * @see [[com.twitter.finagle.dispatch.GenSerialServerDispatcher]]
   */
  protected def newDispatcher(
    transport: Transport[In, Out] {
      type Context <: self.Context
    }
  ): Service[Req, Rep]

  /**
   * A copy constructor in lieu of defining StackClient as a
   * case class.
   */
  override protected def copy1(
    stack: Stack[ServiceFactory[Req, Rep]] = this.stack,
    params: Stack.Params = this.params
  ): This { type In = self.In; type Out = self.Out }

  /**
   * A stackable module that creates new `Transports` (via transporter)
   * when applied.
   */
  protected final def endpointer: Stackable[ServiceFactory[Req, Rep]] =
    new EndpointerModule[Req, Rep](
      Seq(implicitly[Stack.Param[ProtocolLibrary]], implicitly[Stack.Param[Label]]),
      { (prms: Stack.Params, sa: SocketAddress) =>
        val endpointClient = self.copy1(params = prms)
        val transporter = endpointClient.newTransporter(sa)
        // This assumes that the `toString` of the implementation is sufficiently descriptive.
        // Note: this should be kept in sync with the equivalent `PushStackClient` logic.
        endpointClient.registerTransporter(transporter.toString)
        // Note, this ensures that session establishment is lazy (i.e.,
        // on the service acquisition path).
        ServiceFactory.apply[Req, Rep] { () =>
          // we do not want to capture and request specific Locals
          // that would live for the life of the session.
          Contexts.letClearAll {
            transporter().map { trans => endpointClient.newDispatcher(trans) }
          }
        }
      }
    )
}
