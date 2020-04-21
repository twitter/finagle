package com.twitter.finagle.postgresql

import com.twitter.finagle.Stack
import com.twitter.finagle.dispatch.ClientDispatcher.wrapWriteException
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.param.Stats
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.postgresql.Params.Credentials
import com.twitter.finagle.postgresql.Params.Database
import com.twitter.finagle.postgresql.Response.BackendResponse
import com.twitter.finagle.postgresql.Types.Name
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

class ClientDispatcher(
  transport: Transport[Packet, Packet],
  params: Stack.Params,
) extends GenSerialClientDispatcher[Request, Response, Packet, Packet](
  transport,
  params[Stats].statsReceiver
) {

  def write[M <: FrontendMessage](msg: M)(implicit encoder: MessageEncoder[M]): Future[Unit] =
    transport
      .write(encoder.toPacket(msg))
      .rescue {
        case exc => wrapWriteException(exc)
      }

  def read(): Future[BackendMessage] =
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
        // TODO: this value may be necessary to keep around
        case Return(ready) =>
          Future.Done
        case Throw(e) =>
          promise.raise(e)
          // the state machine failed unexpectedly, which leaves the connection in a bad state
          //   let's close the transport
          // TODO: is this the appropriate way to handle "bad connections" in finagle?
          close()
      }
  }

  val handshakeResult: Promise[Response.HandshakeResult] = new Promise()

  val startup = machineDispatch(HandshakeMachine(params[Credentials], params[Database]), handshakeResult)

  override def apply(req: Request): Future[Response] =
    startup before { super.apply(req) }

  override protected def dispatch(req: Request, p: Promise[Response]): Future[Unit] =
    req match {
      case Request.Sync => machineDispatch(StateMachine.singleMachine("SyncMachine", FrontendMessage.Sync)(BackendResponse(_)), p)
      case Request.Query(q) => machineDispatch(new SimpleQueryMachine(q), p)
      case Request.Prepare(s) => machineDispatch(new PrepareMachine(Name.Unnamed, s), p)
      case Request.Execute(prepared, parameters) => machineDispatch(new ExtendedQueryMachine(prepared.name, parameters), p)
    }
}
