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

trait StateMachine[R] {
  def init: StateMachine.TransitionResult[R]
  def receive(msg: BackendMessage): StateMachine.TransitionResult[R]
  def send(msg: FrontendMessage): StateMachine.TransitionResult[R]
}
object StateMachine {
  sealed trait TransitionResult[+R]
  case object Noop extends TransitionResult[Nothing]
  case class Send(msg: FrontendMessage) extends TransitionResult[Nothing]
  case class Complete[R](value: R) extends TransitionResult[R]
}

case class MachineRunner[R](transport: Transport[Packet, Packet], machine: StateMachine[R]) {

  private[this] def readMsg = transport.read().map(Messages.BackendMessage.read)

  private[this] def step(transition: StateMachine.TransitionResult[R]): Future[R] = transition match {
    case StateMachine.Send(msg) => transport.write(msg.write) before readAndStep
    case StateMachine.Complete(result) => Future.value(result)
    case StateMachine.Noop => readAndStep
  }
  private[this] def readAndStep =
    readMsg.flatMap { msg => step(machine.receive(msg)) }

  def run: Future[R] =
    step(machine.init)
}

case class AuthenticationMachine(credentials: Params.Credentials, database: Params.Database) extends StateMachine[HandshakeResult] {

  sealed trait State
  case object Startup extends State
  case object Done extends State
  case object Authenticating extends State
  case class BackendStarting(params: List[ParameterStatus], bkd: Option[BackendKeyData]) extends State

  var state: State = Startup

  override def init: StateMachine.TransitionResult[HandshakeResult] =
    StateMachine.Send(Messages.StartupMessage(user = credentials.username, database = database.name))

  override def receive(msg: BackendMessage): StateMachine.TransitionResult[HandshakeResult] = (state, msg) match {
    case (Startup, Messages.AuthenticationMD5Password(salt)) =>
      def hex(input: Array[Byte]) = input.map(s => f"$s%02x").mkString
      def bytes(str: String) = str.getBytes(StandardCharsets.UTF_8)
      def md5(input: Array[Byte]*): String =
        hex(input.foldLeft(MessageDigest.getInstance("MD5")) { case(d,v) => d.update(v);d }.digest())

      val hashed = md5(
        bytes(md5(bytes(credentials.password), bytes(credentials.username))),
        Buf.ByteArray.Owned.extract(salt)
      )

      state = Authenticating
      StateMachine.Send(Messages.PasswordMessage(s"md5$hashed"))
    case (Authenticating, Messages.AuthenticationOk) =>
      state = BackendStarting(Nil, None)
      StateMachine.Noop
    case (BackendStarting(params, bkd), p: Messages.ParameterStatus) =>
      state = BackendStarting(p :: params, bkd)
      StateMachine.Noop
    case (BackendStarting(params, _), bkd: Messages.BackendKeyData) =>
      state = BackendStarting(params, Some(bkd))
      StateMachine.Noop
    case (BackendStarting(params, bkd), _: Messages.ReadyForQuery) =>
      state = Done
      StateMachine.Complete(HandshakeResult(params, bkd.get))
    case _ =>
      sys.error(s"Unexpected msg $msg in state $state")
  }

  override def send(msg: FrontendMessage): StateMachine.TransitionResult[HandshakeResult] =
    sys.error("unexpected frontend message")
}

case class HandshakeResult(parameters: List[ParameterStatus], backendData: BackendKeyData)

case class Handshake(
  params: Stack.Params,
  transport: Transport[Packet, Packet]
) {

  val credentials = params[Params.Credentials]
  val database = params[Params.Database]

  val machine = AuthenticationMachine(params[Params.Credentials], params[Params.Database])

  def startup(): Future[HandshakeResult] =
    MachineRunner(transport, machine).run
}
