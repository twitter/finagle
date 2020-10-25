package com.twitter.finagle.postgresql

import com.twitter.finagle.Stack
import com.twitter.finagle.dispatch.ClientDispatcher.wrapWriteException
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.param.Stats
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.postgresql.Params.Credentials
import com.twitter.finagle.postgresql.Params.Database
import com.twitter.finagle.postgresql.machine.ExtendedQueryMachine
import com.twitter.finagle.postgresql.machine.HandshakeMachine
import com.twitter.finagle.postgresql.machine.PrepareMachine
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

  /**
   * Send a single message to the backend.
   */
  private[this] def send[M <: FrontendMessage](s: StateMachine.Send[M]): Future[Unit] =
    transport
      .write(s.encoder.toPacket(s.msg))
      .rescue {
        case exc => wrapWriteException(exc)
      }

  /**
   * Read a single message from the backend.
   */
  private[this] def receive(): Future[BackendMessage] =
    transport.read().map(rep => MessageDecoder.fromPacket(rep)).lowerFromTry // TODO: better error handling

  private[this] def run[R <: Response](machine: StateMachine[R], promise: Promise[R]) = {

    // NOTE: this is initialized to null, but is immediately set to the start state before doing anything else.
    var state: machine.State = null.asInstanceOf[machine.State]

    def step(transition: StateMachine.TransitionResult[machine.State, R]): Future[ReadyForQuery] = transition match {
      case StateMachine.Transition(s, action) =>
        state = s
        val doAction = action match {
          case StateMachine.NoOp => Future.Done
          case s@StateMachine.Send(_) => send(s)
          case StateMachine.SendSeveral(msgs) => Future.traverseSequentially(msgs) {
            case s@StateMachine.Send(_) => send(s)
          }.unit
          case StateMachine.Respond(r) =>
            promise.updateIfEmpty(r)
            Future.Done
        }
        doAction before readAndStep
      case StateMachine.Complete(ready, response) =>
        response.foreach(promise.updateIfEmpty)

        // Make sure the state machine produced a response for the client.
        if(!promise.isDefined) {
          // NOTE: we still use updateIfEmpty since the promise may still be canceled.
          promise.updateIfEmpty(
            Throw(PgSqlInvalidMachineStateError(s"State machine ${machine.getClass} was in state $state and completed without producing a response for the client."))
          )
        }
        Future.value(ready)
    }

    def readAndStep: Future[ReadyForQuery] =
      receive().flatMap { msg => step(machine.receive(state, msg)) }

    step(machine.start)
  }

  /**
   * Runs a state machine to completion and fulfills the client response.
   */
  private[this] def machineDispatch[R <: Response](machine: StateMachine[R], promise: Promise[R]): Future[Unit] = {
    run(machine, promise)
      .transform {
        case Return(_) => Future.Done
        case t: Throw[_] =>
          promise.updateIfEmpty(t.cast)
          // the state machine failed unexpectedly, which leaves the connection in a bad state
          //   let's close the transport
          // TODO: is this the appropriate way to handle "bad connections" in finagle?
          close()
      }
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
  private[this] val startup: Future[Unit] = machineDispatch(
    HandshakeMachine(params[Credentials], params[Database]),
    connectionParameters
  )

  // Simple machine for the Sync request which immediately responds with a "ReadyForQuery".
  val syncMachine: StateMachine[Response.Ready.type] =
    StateMachine.singleMachine("SyncMachine", FrontendMessage.Sync)(_ => Response.Ready)

  override def apply(req: Request): Future[Response] =
    startup before { super.apply(req) }

  override protected def dispatch(req: Request, p: Promise[Response]): Future[Unit] = {
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
          case Request.Sync => machineDispatch(syncMachine, p)
          case Request.Query(q) => machineDispatch(new SimpleQueryMachine(q, parameters), p)
          case Request.Prepare(s, name) => machineDispatch(new PrepareMachine(name, s), p)
          case e: Request.Execute => machineDispatch(new ExtendedQueryMachine(e, parameters), p)
        }
    }
  }
}
