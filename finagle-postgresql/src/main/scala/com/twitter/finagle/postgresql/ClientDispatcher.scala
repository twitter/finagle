package com.twitter.finagle.postgresql

import com.twitter.finagle.Stack
import com.twitter.finagle.dispatch.ClientDispatcher.wrapWriteException
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.param.Stats
import com.twitter.finagle.postgresql.Params.Credentials
import com.twitter.finagle.postgresql.Params.Database
import com.twitter.finagle.postgresql.machine.Connection
import com.twitter.finagle.postgresql.machine.ExecuteMachine
import com.twitter.finagle.postgresql.machine.HandshakeMachine
import com.twitter.finagle.postgresql.machine.PrepareMachine
import com.twitter.finagle.postgresql.machine.Runner
import com.twitter.finagle.postgresql.machine.SimpleQueryMachine
import com.twitter.finagle.postgresql.machine.StateMachine
import com.twitter.finagle.postgresql.transport.MessageDecoder
import com.twitter.finagle.postgresql.transport.Packet
import com.twitter.finagle.transport.Transport
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Throw

/**
 * Handles transforming the Postgres protocol to an RPC style.
 *
 * The Postgres protocol is not of the style `request => Future[Response]`.
 * Instead, it uses a stateful protocol where each connection is in a particular state and streams of requests / responses
 * take place to move the connection from one state to another.
 *
 * The dispatcher is responsible for managing this connection state and transforming the stream of request / response to
 * a single request / response style that conforms to Finagle's request / response style.
 *
 * The dispatcher uses state machines to handle the connection state management.
 *
 * When a connection is established, the [[HandshakeMachine]] is immediately executed and takes care of authentication.
 * Subsequent machines to execute are based on the client's query. For example, if the client submits a [[Request.Query]],
 * then the [[SimpleQueryMachine]] will be dispatched to manage the connection's state.
 *
 * Any unexpected error from the state machine will lead to tearing down the connection to make sure we don't
 * reuse a connection in an unknown / bad state.
 *
 * @see [[StateMachine]]
 */
class ClientDispatcher(
  transport: Transport[Packet, Packet],
  params: Stack.Params,
) extends GenSerialClientDispatcher[Request, Response, Packet, Packet](
      transport,
      params[Stats].statsReceiver
    ) {

  // implements Connection on Transport
  private[this] val transportConnection = new Connection {
    def send[M <: FrontendMessage](s: StateMachine.Send[M]): Future[Unit] =
      transport
        .write(s.encoder.toPacket(s.msg))
        .rescue {
          case exc => wrapWriteException(exc)
        }

    def receive(): Future[BackendMessage] =
      transport.read().map(rep => MessageDecoder.fromPacket(rep)).lowerFromTry
  }

  private[this] val machineRunner: Runner = new Runner(transportConnection)
  def machineDispatch[R <: Response](machine: StateMachine[R], promise: Promise[R]): Future[Unit] =
    machineRunner.dispatch(machine, promise)
      .transform {
        case Return(_) => Future.Done
        case Throw(_) => close()
      }

  /**
   * This is used to keep the result of the startup sequence.
   *
   * The startup sequence is initiated when the connection is established, so there's no client response to fulfill.
   * Instead, we fulfill this promise to keep the data available subsequently.
   */
  private[this] val connectionParameters: Promise[Response.ConnectionParameters] = new Promise()

  /**
   * Immediately start the handshaking upon connection establishment, before any client requests.
   */
  private[this] val startup: Future[Unit] = machineRunner.dispatch(
    HandshakeMachine(params[Credentials], params[Database]),
    connectionParameters
  )

  override def apply(req: Request): Future[Response] =
    startup before super.apply(req)

  override protected def dispatch(req: Request, p: Promise[Response]): Future[Unit] =
    connectionParameters.poll match {
      case None => Future.exception(new PgSqlClientError("Handshake result should be available at this point."))
      case Some(Throw(t)) =>
        // If handshaking failed, we cannot proceed with sending requests
        p.setException(t)
        close() // TODO: is it okay to close the connection here?
      case Some(Return(parameters)) =>
        req match {
          case Request.ConnectionParameters =>
            p.setValue(parameters)
            Future.Done
          case Request.Sync => machineDispatch(StateMachine.syncMachine, p)
          case Request.Query(q) => machineDispatch(new SimpleQueryMachine(q, parameters), p)
          case Request.Prepare(s, name) => machineDispatch(new PrepareMachine(name, s), p)
          case e: Request.Execute => machineDispatch(new ExecuteMachine(e, parameters), p)
        }
    }
}
