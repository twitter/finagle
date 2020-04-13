package com.twitter.finagle.postgresql

import com.twitter.finagle.Stack
import com.twitter.finagle.dispatch.ClientDispatcher.wrapWriteException
import com.twitter.finagle.dispatch.GenSerialClientDispatcher
import com.twitter.finagle.param.Stats
import com.twitter.finagle.postgresql.Params.Credentials
import com.twitter.finagle.postgresql.Params.Database
import com.twitter.finagle.postgresql.Response.BackendResponse
import com.twitter.finagle.postgresql.machine.HandshakeMachine
import com.twitter.finagle.postgresql.machine.SimpleQueryMachine
import com.twitter.finagle.postgresql.machine.StateMachine
import com.twitter.finagle.postgresql.transport.MessageDecoder
import com.twitter.finagle.postgresql.transport.MessageEncoder
import com.twitter.finagle.postgresql.transport.Packet
import com.twitter.finagle.transport.Transport
import com.twitter.util.Future
import com.twitter.util.Promise

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

  def exchange[M <: FrontendMessage : MessageEncoder](msg: M): Future[BackendMessage] =
    write(msg) before read()

  def run[R <: Response](machine: StateMachine[R]) = {

    var state: machine.State = null.asInstanceOf[machine.State] // TODO

    def step(transition: StateMachine.TransitionResult[machine.State, R]): Future[StateMachine.Respond[R]] = transition match {
      case StateMachine.Transition(s) =>
        state = s
        readAndStep
      case t@StateMachine.TransitionAndSend(s, msg) =>
        state = s
        write(msg)(t.encoder) before readAndStep
      case r: StateMachine.Respond[R] => Future.value(r)
    }

    def readAndStep =
      read().flatMap { msg => step(machine.receive(state, msg)) }

    step(machine.start)
  }

  def machineDispatch[R <: Response](machine: StateMachine[R], promise: Promise[R]): Future[BackendMessage.ReadyForQuery] = {
    run(machine)
      .flatMap { case StateMachine.Respond(response, signal) =>
        promise.updateIfEmpty(response)
        signal
      }
  }

  val handshakeResult: Promise[Response.HandshakeResult] = new Promise()

  val startup = machineDispatch(HandshakeMachine(params[Credentials], params[Database]), handshakeResult).unit

  override def apply(req: Request): Future[Response] =
    startup before { super.apply(req) }

  override protected def dispatch(req: Request, p: Promise[Response]): Future[Unit] =
    req match {
      case Sync => machineDispatch(StateMachine.singleMachine(FrontendMessage.Sync)(BackendResponse(_)), p).unit
      case Query(q) => machineDispatch(new SimpleQueryMachine(q), p).unit
    }
}
