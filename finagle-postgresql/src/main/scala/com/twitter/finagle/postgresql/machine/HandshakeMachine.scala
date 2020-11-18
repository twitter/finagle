package com.twitter.finagle.postgresql.machine

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationCleartextPassword
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationGSS
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationKerberosV5
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationMD5Password
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationOk
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationSASL
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationSCMCredential
import com.twitter.finagle.postgresql.BackendMessage.AuthenticationSSPI
import com.twitter.finagle.postgresql.BackendMessage.NoTx
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.postgresql.FrontendMessage
import com.twitter.finagle.postgresql.Params
import com.twitter.finagle.postgresql.PgSqlNoSuchTransition
import com.twitter.finagle.postgresql.PgSqlPasswordRequired
import com.twitter.finagle.postgresql.PgSqlServerError
import com.twitter.finagle.postgresql.PgSqlUnsupportedAuthenticationMechanism
import com.twitter.finagle.postgresql.Response
import com.twitter.io.Buf
import com.twitter.util.Return
import com.twitter.util.Throw

/**
 * Implements the "Start-up" message flow described here https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.3
 *
 * This process involves authenticating the client and accumulating parameters about the server's configuration for this connection.
 * Failure to authenticate will produce an exception.
 * A successful response [[Response.ConnectionParameters]] which includes the connection's parameters
 * such as character encoding and timezone.
 */
case class HandshakeMachine(credentials: Params.Credentials, database: Params.Database)
    extends StateMachine[Response.ConnectionParameters] {

  import StateMachine._

  sealed trait State
  case object Authenticating extends State
  case class BackendStarting(params: List[BackendMessage.ParameterStatus], bkd: Option[BackendMessage.BackendKeyData])
      extends State

  override def start: StateMachine.TransitionResult[State, Response.ConnectionParameters] =
    Transition(
      Authenticating,
      Send(FrontendMessage.StartupMessage(user = credentials.username, database = database.name))
    )

  override def receive(
    state: State,
    msg: BackendMessage
  ): StateMachine.TransitionResult[State, Response.ConnectionParameters] = (state, msg) match {
    case (Authenticating, AuthenticationMD5Password(salt)) =>
      def hex(input: Array[Byte]) = input.map(s => f"$s%02x").mkString
      def bytes(str: String) = str.getBytes(StandardCharsets.UTF_8)
      def md5(input: Array[Byte]*): String =
        hex(input.foldLeft(MessageDigest.getInstance("MD5")) { case (d, v) => d.update(v); d }.digest())

      credentials.password match {
        case None => Complete(ReadyForQuery(NoTx), Some(Throw(PgSqlPasswordRequired)))
        case Some(password) =>
          // concat('md5', md5(concat(md5(concat(password, username)), random-salt)))
          val hashed = md5(
            bytes(md5(bytes(password), bytes(credentials.username))),
            Buf.ByteArray.Owned.extract(salt)
          )

          Transition(Authenticating, Send(FrontendMessage.PasswordMessage(s"md5$hashed")))
      }

    case (Authenticating, AuthenticationCleartextPassword) =>
      credentials.password match {
        case None => Complete(ReadyForQuery(NoTx), Some(Throw(PgSqlPasswordRequired)))
        case Some(password) => Transition(Authenticating, Send(FrontendMessage.PasswordMessage(password)))
      }

    case (Authenticating, AuthenticationOk) => // This can happen at Startup when there's no password
      Transition(BackendStarting(Nil, None), NoOp)

    case (BackendStarting(params, bkd), p: BackendMessage.ParameterStatus) =>
      Transition(BackendStarting(p :: params, bkd), NoOp)
    case (BackendStarting(params, _), bkd: BackendMessage.BackendKeyData) =>
      Transition(BackendStarting(params, Some(bkd)), NoOp)

    case (BackendStarting(params, bkd), ready: BackendMessage.ReadyForQuery) =>
      Complete(ready, Some(Return(Response.ConnectionParameters(params, bkd))))

    case (state, _: BackendMessage.NoticeResponse) => Transition(state, NoOp) // TODO: don't ignore
    case (_, e: BackendMessage.ErrorResponse) =>
      // The backend closes the connection, so we use a bogus ReadyForQuery value
      Complete(ReadyForQuery(NoTx), Some(Throw(PgSqlServerError(e))))

    case (
          _,
          AuthenticationGSS |
          AuthenticationKerberosV5 |
          AuthenticationSCMCredential |
          AuthenticationSSPI |
          AuthenticationSASL(_)
        ) =>
      Complete(
        ReadyForQuery(NoTx),
        Some(Throw(PgSqlUnsupportedAuthenticationMechanism(msg.asInstanceOf[BackendMessage.AuthenticationMessage])))
      )

    case (state, msg) => throw PgSqlNoSuchTransition("HandshakeMachine", state, msg)
  }
}
