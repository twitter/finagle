package com.twitter.finagle

import com.twitter.finagle.client.StackClient
import com.twitter.finagle.client.StdStackClient
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.filter.NackAdmissionFilter
import com.twitter.finagle.param.WithSessionPool
import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.DelayedRelease
import com.twitter.finagle.postgresql.FrontendMessage
import com.twitter.finagle.postgresql.Params
import com.twitter.finagle.postgresql.Request
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.service.TimeoutFilter
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.TransportContext
import com.twitter.util.Duration
import com.twitter.util.tunable.Tunable
import java.net.SocketAddress

object PostgreSql {

  val defaultStack: Stack[ServiceFactory[Request, Response]] =
    StackClient
      .newStack[Request, Response]
      // we'll handle timeouts on our own
      .remove(TimeoutFilter.role)
      // We add a DelayedRelease module at the bottom of the stack to ensure
      // that the pooling levels above don't discard an active session.
      .replace(StackClient.Role.prepConn, DelayedRelease.module(StackClient.Role.prepConn))
      .replace(
        StackClient.Role.requestDraining,
        DelayedRelease.module(StackClient.Role.requestDraining))
      // Since NackAdmissionFilter should operate on all requests sent over
      // the wire including retries, it must be below `Retries`. Since it
      // aggregates the status of the entire cluster, it must be above
      // `LoadBalancerFactory` (not part of the endpoint stack).
      .insertBefore(
        StackClient.Role.prepFactory,
        NackAdmissionFilter.module[Request, Response]
      )

  val defaultParams: Stack.Params = StackClient.defaultParams +
    // Keep NackAdmissionFilter disabled by default for backwards compatibility.
    NackAdmissionFilter.Disabled

  case class Client(
    stack: Stack[ServiceFactory[Request, Response]] = defaultStack,
    params: Stack.Params = defaultParams)
      extends StdStackClient[Request, Response, Client]
      with WithSessionPool[Client] {

    override type In = FrontendMessage
    override type Out = BackendMessage
    override type Context = TransportContext

    type ClientTransport = Transport[In, Out] { type Context <: TransportContext }

    def withCredentials(u: String, p: Option[String]): Client =
      configured(Params.Credentials(u, p))

    def withDatabase(db: String): Client =
      configured(Params.Database(Some(db)))

    def newRichClient(dest: String): postgresql.Client = {
      val timeoutFn = params[TimeoutFilter.Param].timeout _
      val timer = params[com.twitter.finagle.param.Timer].timer
      postgresql.Client(newClient(dest), timeoutFn)(timer)
    }

    /**
     * Configure the client to set a server-side statement timeout.  This is the equivalent of
     * `set session statement_timeout = N`
     */
    def withStatementTimeout(timeout: Duration): Client =
      configured(Params.StatementTimeout(timeout))

    /**
     * Configure the client to set a dynamic server-side statement timeout. The input tunable is evaluated each time a
     * new connection is established.  This is the equivalent of `set session statement_timeout = N`
     */
    def withStatementTimeout(timeout: Tunable[Duration]): Client =
      configured(Params.StatementTimeout(timeout))

    /**
     * Configure the client to send `cmds` after a new connection is established.
     *
     * @note `cmds` must be commands, statements that return result sets will cause the connection establishment to
     *       fail.
     */
    def withConnectionInitializationCommands(cmds: Seq[String]): Client =
      configured(Params.ConnectionInitializationCommands(cmds))

    /**
     * Configure the client to set the session variables specified in `defaults`.
     *
     * @note Unknown session variables will be ignored by the server.
     */
    def withSessionDefaults(defaults: Map[String, String]): Client =
      configured(Params.SessionDefaults(defaults))

    override protected def newTransporter(
      addr: SocketAddress
    ): Transporter[FrontendMessage, BackendMessage, TransportContext] =
      new postgresql.PgSqlTransporter(addr, params)

    override protected def newDispatcher(transport: ClientTransport): Service[Request, Response] =
      postgresql.ClientDispatcher.cached(transport, params)

    override protected def copy1(
      stack: Stack[ServiceFactory[Request, Response]] = this.stack,
      params: Stack.Params = this.params
    ): Client = copy(stack, params)
  }

  def client: Client = Client()
}
