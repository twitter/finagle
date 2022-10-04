package com.twitter.finagle.pushsession

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.server.ListeningStackServer
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.util.DefaultTimer
import com.twitter.finagle.ClientConnection
import com.twitter.finagle.ClientConnectionProxy
import com.twitter.finagle.Failure
import com.twitter.finagle.ListeningServer
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Throw
import java.net.SocketAddress

/**
 * Implementation of [[ListeningStackServer]] which uses the push-based abstractions.
 */
trait PushStackServer[Req, Rep, This <: PushStackServer[Req, Rep, This]]
    extends ListeningStackServer[Req, Rep, This] { self: This =>

  /**
   * The type we write into the channel.
   */
  protected type PipelineRep

  /**
   * The type we read out of the channel.
   */
  protected type PipelineReq

  /**
   * Create a new instance of a [[PushListener]] which will be used to accept connections
   */
  protected def newListener(): PushListener[PipelineReq, PipelineRep]

  /**
   * Make a new [[PushSession]] with the provided [[PushChannelHandle]] and [[Service]]
   *
   * @note it is presumed that the new session will take ownership of both the handle
   *       and the service and it is the sessions job to release these resources.
   *
   * @note It is possible that the new session will receive a handle that has already
   *       been closed, and must free its resources under such circumstances.
   */
  protected def newSession(
    handle: PushChannelHandle[PipelineReq, PipelineRep],
    service: Service[Req, Rep]
  ): PushSession[PipelineReq, PipelineRep]

  final protected def newListeningServer(
    serviceFactory: ServiceFactory[Req, Rep],
    addr: SocketAddress
  )(
    trackSession: ClientConnection => Unit
  ): ListeningServer = {

    val listener = newListener()

    // This assumes that the `toString` of the implementation is sufficiently descriptive.
    addServerToRegistry(listener.toString)

    listener.listen(addr) { handle: PushChannelHandle[PipelineReq, PipelineRep] =>
      val conn = new ClientConnectionProxy(handle)

      def mkSession(svc: Service[Req, Rep]): PushSession[PipelineReq, PipelineRep] = {
        val session = newSession(handle, svc)

        // Since the session is required to release its resources if it receives
        // a closed handle there is no need to ensure resource cleanup if the
        // proxy has already had its `close` invoked.
        conn.trySetClosable(session)
        trackSession(conn)
        session
      }

      // Note, this will not work for OppTls since `sslSessionInfo`
      // isn't set at this point. It will, however, propagate the correct
      // `sslSessionInfo` during service acquisition for standard Tls.
      val futureService = Contexts.local.let(Transport.sslSessionInfoCtx, handle.sslSessionInfo) {
        serviceFactory(conn)
      }

      // We transform to make sure we execute the block before the Future resolves
      futureService.transform {
        case Return(svc) =>
          val session = mkSession(svc)
          Future.value(session)

        case Throw(exc) =>
          // If we fail to create a new session locally, we continue establishing
          // the session but (1) reject any incoming requests; (2) close it right
          // away. This allows protocols that support graceful shutdown to
          // also gracefully deny new sessions.
          val svc = Service.const(
            Future.exception(Failure.rejected("Terminating session and rejecting request", exc))
          )
          val session = mkSession(svc)
          // Delay closing the session to give the server time for negotiation during session
          // initialization.
          DefaultTimer.doLater(10.seconds) {
            // We provide 10 seconds for the session to drain gracefully which should
            // be enough time to signal draining and nack any racing requests.
            session.close(10.seconds)
          }
          Future.value(session)
      }
    }
  }
}
