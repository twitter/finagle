package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.postgresql.FrontendMessage
import com.twitter.finagle.postgresql.PgSqlServerError
import com.twitter.finagle.postgresql.PgSqlStateMachineError
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.transport.MessageEncoder
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try

trait StateMachine[+R <: Response] {
  type State
  def start: StateMachine.TransitionResult[State, R]
  def receive(state: State, msg: BackendMessage): StateMachine.TransitionResult[State, R]
}
object StateMachine {
  sealed trait TransitionResult[+S, +R <: Response]
  case class TransitionAndSend[S, M <: FrontendMessage](state: S, msg: M)(implicit val encoder: MessageEncoder[M]) extends TransitionResult[S, Nothing]
  case class Transition[S](state: S) extends TransitionResult[S, Nothing]
  case class Respond[S, R <: Response](state: S, value: Try[R]) extends TransitionResult[S, R]
  case class Complete[R <: Response](ready: ReadyForQuery, response: Option[Try[R]]) extends TransitionResult[Nothing, R]

  /** A machine that sends a single frontend message and expects a ReadyForQuery response */
  def singleMachine[M <: FrontendMessage: MessageEncoder, R <: Response](msg: M)(f: BackendMessage.ReadyForQuery => R): StateMachine[R] = new StateMachine[R] {
    override type State = Unit
    override def start: TransitionResult[State, R] = TransitionAndSend((), msg)
    override def receive(state: State, msg: BackendMessage): TransitionResult[State, R] = msg match {
      case r: BackendMessage.ReadyForQuery => Complete(r, Some(Return(f(r))))
      case e: BackendMessage.ErrorResponse => Respond(Unit, Throw(PgSqlServerError(e)))
      case msg => throw PgSqlStateMachineError("anonymous", (), msg)
    }
  }
}
