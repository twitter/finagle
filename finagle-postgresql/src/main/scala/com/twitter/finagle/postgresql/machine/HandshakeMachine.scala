package com.twitter.finagle.postgresql.machine

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.BackendMessage.NoTx
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.postgresql.FrontendMessage
import com.twitter.finagle.postgresql.Params
import com.twitter.finagle.postgresql.PgSqlServerError
import com.twitter.finagle.postgresql.PgSqlStateMachineError
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.Response.HandshakeResult
import com.twitter.io.Buf
import com.twitter.util.Return
import com.twitter.util.Throw

case class HandshakeMachine(credentials: Params.Credentials, database: Params.Database) extends StateMachine[Response.HandshakeResult] {

  sealed trait State
  case object Authenticating extends State
  case class BackendStarting(params: List[BackendMessage.ParameterStatus], bkd: Option[BackendMessage.BackendKeyData]) extends State

  override def start: StateMachine.TransitionResult[State, Response.HandshakeResult] =
    StateMachine.TransitionAndSend(Authenticating, FrontendMessage.StartupMessage(user = credentials.username, database = database.name))

  override def receive(state: State, msg: BackendMessage): StateMachine.TransitionResult[State, Response.HandshakeResult] = (state, msg) match {
    case (Authenticating, BackendMessage.AuthenticationMD5Password(salt)) =>
      def hex(input: Array[Byte]) = input.map(s => f"$s%02x").mkString
      def bytes(str: String) = str.getBytes(StandardCharsets.UTF_8)
      def md5(input: Array[Byte]*): String =
        hex(input.foldLeft(MessageDigest.getInstance("MD5")) { case(d,v) => d.update(v);d }.digest())

      val hashed = md5(
        bytes(md5(bytes(credentials.password.getOrElse("")), bytes(credentials.username))),
        Buf.ByteArray.Owned.extract(salt)
      )

      StateMachine.TransitionAndSend(Authenticating, FrontendMessage.PasswordMessage(s"md5$hashed"))
    case (Authenticating, BackendMessage.AuthenticationOk) => // This can happen at Startup when there's no password
      StateMachine.Transition(BackendStarting(Nil, None))
    case (BackendStarting(params, bkd), p: BackendMessage.ParameterStatus) =>
      StateMachine.Transition(BackendStarting(p :: params, bkd))
    case (BackendStarting(params, _), bkd: BackendMessage.BackendKeyData) =>
      StateMachine.Transition(state = BackendStarting(params, Some(bkd)))


    case (BackendStarting(params, bkd), ready: BackendMessage.ReadyForQuery) =>
      StateMachine.Complete(ready, Some(Return(HandshakeResult(params, bkd.get))))

    case (state, _: BackendMessage.NoticeResponse) => StateMachine.Transition(state) // TODO: don't ignore
    case (_, e: BackendMessage.ErrorResponse) =>
      // The backend closes the connection, so we use a bogus ReadyForQuery value
      StateMachine.Complete(ReadyForQuery(NoTx), Some(Throw(PgSqlServerError(e))))

    case (state, msg) => throw PgSqlStateMachineError("SimpleQueryMachine", state, msg)
  }
}

