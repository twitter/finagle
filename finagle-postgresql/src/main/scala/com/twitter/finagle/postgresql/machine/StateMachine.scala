package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.FrontendMessage
import com.twitter.finagle.postgresql.PgSqlServerError
import com.twitter.finagle.postgresql.PgSqlStateMachineError
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.transport.MessageEncoder
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try

trait StateMachine[S, +R <: Response] {
  def start: StateMachine.TransitionResult[S, R]
  def receive(state: S, msg: BackendMessage): StateMachine.TransitionResult[S, R]
}
object StateMachine {
  sealed trait TransitionResult[+S, +R <: Response]
  case class TransitionAndSend[S, M <: FrontendMessage](state: S, msg: M)(implicit val encoder: MessageEncoder[M]) extends TransitionResult[S, Nothing]
  case class Transition[S](state: S) extends TransitionResult[S, Nothing]
  case class Respond[R <: Response](value: Try[R], signal: Future[BackendMessage.ReadyForQuery]) extends TransitionResult[Nothing, R]

  /** A machine that sends a single frontend message and expects a ReadyForQuery response */
  def singleMachine[M <: FrontendMessage: MessageEncoder, R <: Response](msg: M)(f: BackendMessage.ReadyForQuery => R): StateMachine[Unit, R] = new StateMachine[Unit, R] {
    override def start: TransitionResult[Unit, R] = TransitionAndSend((), msg)
    override def receive(state: Unit, msg: BackendMessage): TransitionResult[Unit, R] = msg match {
      case r: BackendMessage.ReadyForQuery => Respond(Return(f(r)), Future.value(r))
      case e: BackendMessage.ErrorResponse => Respond(Throw(PgSqlServerError(e)), Future.exception(new RuntimeException)) // TODO: what happens when the signal is an exception?
      case msg => Respond(Throw(PgSqlStateMachineError("anonymous", "n/a", msg)), Future.exception(new RuntimeException))
    }
  }
}
