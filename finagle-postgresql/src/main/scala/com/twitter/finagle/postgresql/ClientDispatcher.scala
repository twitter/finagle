package com.twitter.finagle.postgresql

import java.nio.charset.Charset
import java.time.ZoneId
import java.util.concurrent.atomic.AtomicReference

import com.twitter.finagle.Stack
import com.twitter.finagle.dispatch.ClientDispatcher.wrapWriteException
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.param.Stats
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.postgresql.Params.Credentials
import com.twitter.finagle.postgresql.Params.Database
import com.twitter.finagle.postgresql.Response.BackendResponse
import com.twitter.finagle.postgresql.Response.ConnectionParameters
import com.twitter.finagle.postgresql.machine.ExtendedQueryMachine
import com.twitter.finagle.postgresql.machine.HandshakeMachine
import com.twitter.finagle.postgresql.machine.PrepareMachine
import com.twitter.finagle.postgresql.machine.SimpleQueryMachine
import com.twitter.finagle.postgresql.machine.StateMachine
import com.twitter.finagle.postgresql.transport.MessageDecoder
import com.twitter.finagle.postgresql.transport.MessageEncoder
import com.twitter.finagle.postgresql.transport.Packet
import com.twitter.finagle.transport.Transport
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try

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
  private[this] def write[M <: FrontendMessage](msg: M)(implicit encoder: MessageEncoder[M]): Future[Unit] =
    transport
      .write(encoder.toPacket(msg))
      .rescue {
        case exc => wrapWriteException(exc)
      }

  /**
   * Read a single message from the backend.
   */
  private[this] def read(): Future[BackendMessage] =
    transport.read().map(rep => MessageDecoder.fromPacket(rep)).lowerFromTry // TODO: better error handling

  def run[R <: Response](machine: StateMachine[R], promise: Promise[R]) = {

    var state: machine.State = null.asInstanceOf[machine.State] // TODO

    def step(transition: StateMachine.TransitionResult[machine.State, R]): Future[ReadyForQuery] = transition match {
      case StateMachine.Transition(s, action) =>
        state = s
        val doAction = action match {
          case StateMachine.NoOp => Future.Done
          case a@StateMachine.Send(msg) => write(msg)(a.encoder)
          case StateMachine.SendSeveral(msgs) => Future.traverseSequentially(msgs) {
            case a@StateMachine.Send(msg) => write(msg)(a.encoder)
          }.unit
          case StateMachine.Respond(r) =>
            promise.updateIfEmpty(r)
            Future.Done
        }
        doAction before readAndStep
      case StateMachine.Complete(ready, response) =>
        response.foreach(promise.updateIfEmpty)
        Future.value(ready)
    }

    def readAndStep =
      read().flatMap { msg => step(machine.receive(state, msg)) }

    step(machine.start)
  }

  def machineDispatch[R <: Response](machine: StateMachine[R], promise: Promise[R]): Future[Unit] = {
    run(machine, promise)
      .transform {
        case Return(_) =>
          Future.Done
        case Throw(e) =>
          promise.raise(e)
          // the state machine failed unexpectedly, which leaves the connection in a bad state
          //   let's close the transport
          // TODO: is this the appropriate way to handle "bad connections" in finagle?
          close()
      }
  }

  val connectionParameters = new AtomicReference[Try[ConnectionParameters]]
  val handshakeResult: Promise[Response.HandshakeResult] = new Promise()

  // TODO: this really belongs in the higher-level client.
  handshakeResult
    .map { result =>
      val params = result.parameters
        .map { p =>
          p.key -> p.value
        }
        .toMap

      // make sure the backend uses integers to store date time values.
      // Ancient Postgres versions used double and made this a compilation option.
      // Since Postgres 10, this is "on" by default and cannot be changed.
      // We still check, since this would have dire consequences on timestamp values.
      require(params(BackendMessage.Parameter.IntegerDateTimes) == "on", "integer_datetimes must be on.")

      ConnectionParameters(
        serverEncoding = Charset.forName(params(BackendMessage.Parameter.ServerEncoding)),
        clientEncoding = Charset.forName(params(BackendMessage.Parameter.ClientEncoding)),
        timeZone = ZoneId.of(params(BackendMessage.Parameter.TimeZone))
      )
    }
    .respond(connectionParameters.set)

  val startup: Future[Unit] = machineDispatch(HandshakeMachine(params[Credentials], params[Database]), handshakeResult)

  override def apply(req: Request): Future[Response] =
    startup before { super.apply(req) }

  override protected def dispatch(req: Request, p: Promise[Response]): Future[Unit] = {
    connectionParameters.get() match {
      case null => Future.exception(new PgSqlClientError("Handshake result should be available at this point."))
      case Throw(t) => {
        p.setException(t)
        Future.Done
      }
      case Return(parameters) =>
        req match {
          case Request.ConnectionParameters => handshakeResult.respond(p.update).unit
          case Request.Sync => machineDispatch(StateMachine.singleMachine("SyncMachine", FrontendMessage.Sync)(BackendResponse(_)), p)
          case Request.Query(q) => machineDispatch(new SimpleQueryMachine(q, parameters), p)
          case Request.Prepare(s, name) => machineDispatch(new PrepareMachine(name, s), p)
          case e: Request.Execute => machineDispatch(new ExtendedQueryMachine(e, parameters), p)
        }
    }
  }
}
