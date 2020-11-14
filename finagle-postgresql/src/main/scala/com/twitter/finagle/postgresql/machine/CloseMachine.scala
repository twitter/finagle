package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.BackendMessage.ErrorResponse
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.postgresql.FrontendMessage.Close
import com.twitter.finagle.postgresql.FrontendMessage.DescriptionTarget
import com.twitter.finagle.postgresql.FrontendMessage.Flush
import com.twitter.finagle.postgresql.PgSqlNoSuchTransition
import com.twitter.finagle.postgresql.PgSqlServerError
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.machine.StateMachine.Complete
import com.twitter.finagle.postgresql.machine.StateMachine.Respond
import com.twitter.finagle.postgresql.machine.StateMachine.SendSeveral
import com.twitter.finagle.postgresql.machine.StateMachine.Transition
import com.twitter.finagle.postgresql.machine.StateMachine.TransitionResult
import com.twitter.util.Return
import com.twitter.util.Throw

/**
 * Implements a simple machine for closing a prepared statement or a portal.
 */
class CloseMachine(name: Name, target: DescriptionTarget) extends StateMachine[Response.Ready.type] {

  sealed trait State
  case object Closing extends State
  case object Closed extends State

  override def start: TransitionResult[State, Response.Ready.type] =
    Transition(
      Closing,
      SendSeveral(
        Close(target, name),
        Flush
      )
    )

  override def receive(state: State, msg: BackendMessage): TransitionResult[State, Response.Ready.type] =
    (state, msg) match {
      case (Closing, BackendMessage.CloseComplete) => Transition(Closed, Respond(Return(Response.Ready)))
      case (Closed, r: ReadyForQuery) => Complete(r, None)
      case (_, e: ErrorResponse) => Transition(Closed, Respond(Throw(PgSqlServerError(e))))

      case (state, msg) => throw PgSqlNoSuchTransition("CloseMachine", state, msg)
    }

}
