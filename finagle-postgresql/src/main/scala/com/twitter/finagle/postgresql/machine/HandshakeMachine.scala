package com.twitter.finagle.postgresql.machine

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import com.twitter.finagle.postgresql.Messages
import com.twitter.finagle.postgresql.Messages.BackendKeyData
import com.twitter.finagle.postgresql.Messages.BackendMessage
import com.twitter.finagle.postgresql.Messages.FrontendMessage
import com.twitter.finagle.postgresql.Messages.ParameterStatus
import com.twitter.finagle.postgresql.Params
import com.twitter.io.Buf

case class HandshakeMachine(credentials: Params.Credentials, database: Params.Database) extends StateMachine[HandshakeMachine.State, HandshakeMachine.HandshakeResult] {
  import HandshakeMachine._

  override def init: State = Authenticating

  override def start: StateMachine.TransitionResult[State, HandshakeResult] =
    StateMachine.Send(Authenticating, Messages.StartupMessage(user = credentials.username, database = database.name))

  override def receive(state: State, msg: BackendMessage): StateMachine.TransitionResult[State, HandshakeResult] = (state, msg) match {
    case (Authenticating, Messages.AuthenticationMD5Password(salt)) =>
      def hex(input: Array[Byte]) = input.map(s => f"$s%02x").mkString
      def bytes(str: String) = str.getBytes(StandardCharsets.UTF_8)
      def md5(input: Array[Byte]*): String =
        hex(input.foldLeft(MessageDigest.getInstance("MD5")) { case(d,v) => d.update(v);d }.digest())

      val hashed = md5(
        bytes(md5(bytes(credentials.password.getOrElse("")), bytes(credentials.username))),
        Buf.ByteArray.Owned.extract(salt)
      )

      StateMachine.Send(Authenticating, Messages.PasswordMessage(s"md5$hashed"))
    case (Authenticating, Messages.AuthenticationOk) => // This can happen at Startup when there's no password
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

object HandshakeMachine {
  sealed trait State
  case object Authenticating extends State
  case class BackendStarting(params: List[ParameterStatus], bkd: Option[BackendKeyData]) extends State

  case class HandshakeResult(parameters: List[ParameterStatus], backendData: BackendKeyData)
}
