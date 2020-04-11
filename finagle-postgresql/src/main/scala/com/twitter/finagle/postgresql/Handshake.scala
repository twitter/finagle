package com.twitter.finagle.postgresql

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import com.twitter.finagle.Stack
import com.twitter.finagle.postgresql.Messages.BackendMessage
import com.twitter.finagle.postgresql.Messages.BackendKeyData
import com.twitter.finagle.postgresql.Messages.FrontendMessage
import com.twitter.finagle.postgresql.Messages.ParameterStatus
import com.twitter.finagle.postgresql.transport.Packet
import com.twitter.finagle.transport.Transport
import com.twitter.io.Buf
import com.twitter.util.Future

trait StateMachine[S, R] {
  def init: S
  def start: StateMachine.TransitionResult[S, R]
  def receive(state: S, msg: BackendMessage): StateMachine.TransitionResult[S, R]
  def send(state: S, msg: FrontendMessage): StateMachine.TransitionResult[S, R]
}
object StateMachine {
  sealed trait TransitionResult[+S, +R]
  case class Noop[S](state: S) extends TransitionResult[S, Nothing]
  case class Send[S](state: S, msg: FrontendMessage) extends TransitionResult[S, Nothing]
  case class Complete[R](value: R) extends TransitionResult[Nothing, R] // TODO: how do we move to the next machine?
}

case class MachineRunner[S, R](transport: Transport[Packet, Packet], machine: StateMachine[S, R]) {

  var state: S = machine.init

  private[this] def readMsg = transport.read().map(Messages.BackendMessage.read)

  private[this] def step(transition: StateMachine.TransitionResult[S, R]): Future[R] = transition match {
    case StateMachine.Send(s, msg) =>
      state = s
      transport.write(msg.write) before readAndStep
    case StateMachine.Complete(result) => Future.value(result)
    case StateMachine.Noop(s) =>
      state = s
      readAndStep
  }
  private[this] def readAndStep =
    readMsg.flatMap { msg => step(machine.receive(state, msg)) }

  def run: Future[R] =
    step(machine.start)
}

case class AuthenticationMachine(credentials: Params.Credentials, database: Params.Database) extends StateMachine[AuthenticationMachine.State, HandshakeResult] {
  import AuthenticationMachine._

  override def init: State = Startup

  override def start: StateMachine.TransitionResult[State, HandshakeResult] =
    StateMachine.Send(Startup, Messages.StartupMessage(user = credentials.username, database = database.name))

  override def receive(state: State, msg: BackendMessage): StateMachine.TransitionResult[State, HandshakeResult] = (state, msg) match {
    case (Startup, Messages.AuthenticationMD5Password(salt)) =>
      def hex(input: Array[Byte]) = input.map(s => f"$s%02x").mkString
      def bytes(str: String) = str.getBytes(StandardCharsets.UTF_8)
      def md5(input: Array[Byte]*): String =
        hex(input.foldLeft(MessageDigest.getInstance("MD5")) { case(d,v) => d.update(v);d }.digest())

      val hashed = md5(
        bytes(md5(bytes(credentials.password.getOrElse("")), bytes(credentials.username))),
        Buf.ByteArray.Owned.extract(salt)
      )

      StateMachine.Send(Authenticating, Messages.PasswordMessage(s"md5$hashed"))
    case (Authenticating | Startup, Messages.AuthenticationOk) => // This can happen at Startup when there's no password
      StateMachine.Noop(BackendStarting(Nil, None))
    case (BackendStarting(params, bkd), p: Messages.ParameterStatus) =>
      StateMachine.Noop(BackendStarting(p :: params, bkd))
    case (BackendStarting(params, _), bkd: Messages.BackendKeyData) =>
      StateMachine.Noop(state = BackendStarting(params, Some(bkd)))
    case (BackendStarting(params, bkd), _: Messages.ReadyForQuery) =>
      StateMachine.Complete(HandshakeResult(params, bkd.get))
    case _ =>
      sys.error(s"Unexpected msg $msg in state $state")
  }

  override def send(state: State, msg: FrontendMessage): StateMachine.TransitionResult[State, HandshakeResult] =
    sys.error("unexpected frontend message")
}

object AuthenticationMachine {
  sealed trait State
  case object Startup extends State
  case object Authenticating extends State
  case class BackendStarting(params: List[ParameterStatus], bkd: Option[BackendKeyData]) extends State
}

case class HandshakeResult(parameters: List[ParameterStatus], backendData: BackendKeyData)

case class Handshake(
  params: Stack.Params,
  transport: Transport[Packet, Packet]
) {

  def startup(): Future[HandshakeResult] =
    MachineRunner(transport, AuthenticationMachine(params[Params.Credentials], params[Params.Database])).run
}
