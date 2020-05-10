package com.twitter.finagle.pushsession

import com.twitter.finagle.{Service, ServiceFactory, Stack, Stackable}
import com.twitter.finagle.client.{EndpointerModule, EndpointerStackClient}
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.param.{Label, ProtocolLibrary}
import com.twitter.util.Future
import java.net.SocketAddress

/**
 * Base type for building a [[com.twitter.finagle.client.StackClient]] using
 * the push-based protocol tools.
 */
abstract class PushStackClient[Req, Rep, This <: PushStackClient[Req, Rep, This]]
    extends EndpointerStackClient[Req, Rep, This] { self =>

  /**
   * Type of the messages which will be received from the transporter level
   * by the [[PushSession]].
   */
  protected type In

  /**
   * Type of messages which will be written to the transporter level by
   * the [[PushSession]].
   */
  protected type Out

  /**
   * Refined type of the [[PushSession]].
   *
   * @note the type of `SessionT` is significant as it represents
   * the type which will `receive` the inbound events from the
   * handle. It is possible to transition between session implementations
   * in order to handle protocol transitions (e.g. negotiation).
   * For example, `newSession` can return the type which will receive
   * the first phase of inbound events and `toService` can transform
   * the type via future composition. In this way, the relationship
   * between `newSession` and `toService` represent a state
   * machine transition.
   */
  protected type SessionT <: PushSession[In, Out]

  override protected def copy1(stack: Stack[ServiceFactory[Req, Rep]], params: Stack.Params): This {
    type In = self.In; type Out = self.Out; type SessionT = self.SessionT
  }

  /**
   * Construct a new [[PushTransporter]] with the appropriately configured pipeline.
   */
  protected def newPushTransporter(sa: SocketAddress): PushTransporter[In, Out]

  /**
   * Construct a new push session from the provided [[PushChannelHandle]] generated
   * from the [[PushTransporter]]
   */
  protected def newSession(handle: PushChannelHandle[In, Out]): Future[SessionT]

  /**
   * Build a `Service` from the provided push session
   */
  protected def toService(session: SessionT): Future[Service[Req, Rep]]

  final protected def endpointer: Stackable[ServiceFactory[Req, Rep]] =
    new EndpointerModule[Req, Rep](
      Seq(implicitly[Stack.Param[ProtocolLibrary]], implicitly[Stack.Param[Label]]),
      { (prms: Stack.Params, sa: SocketAddress) =>
        val endpointClient = self.copy1(params = prms)
        val transporter = endpointClient.newPushTransporter(sa)
        // This assumes that the `toString` of the implementation is sufficiently descriptive.
        // Note: this should be kept in sync with the equivalent `StdStackClient` logic.
        endpointClient.registerTransporter(transporter.toString)
        ServiceFactory.apply[Req, Rep] { () =>
          // we do not want to capture and request specific Locals
          // that would live for the life of the session.
          Contexts.letClearAll {
            transporter(endpointClient.newSession).flatMap(endpointClient.toService)
          }
        }
      }
    )

}
