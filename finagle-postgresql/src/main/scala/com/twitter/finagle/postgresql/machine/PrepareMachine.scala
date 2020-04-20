package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.BackendMessage.ErrorResponse
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.postgresql.FrontendMessage
import com.twitter.finagle.postgresql.FrontendMessage.Sync
import com.twitter.finagle.postgresql.PgSqlNoSuchTransition
import com.twitter.finagle.postgresql.PgSqlServerError
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.machine.StateMachine.Complete
import com.twitter.finagle.postgresql.machine.StateMachine.Respond
import com.twitter.finagle.postgresql.machine.StateMachine.Send
import com.twitter.finagle.postgresql.machine.StateMachine.Transition
import com.twitter.finagle.postgresql.machine.StateMachine.TransitionResult
import com.twitter.util.Return
import com.twitter.util.Throw

class PrepareMachine(name: Name, statement: String) extends StateMachine[Response.ParseComplete] {

  sealed trait State
  case object Parsing extends State
  case object Syncing extends State

  override def start: TransitionResult[State, Response.ParseComplete] =
    Transition(Parsing, Send(FrontendMessage.Parse(name, statement, Nil), flush = true))

  override def receive(state: State, msg: BackendMessage): TransitionResult[State, Response.ParseComplete] = (state, msg) match {
    case (Parsing, BackendMessage.ParseComplete) => Transition(Syncing, Send(Sync))
    case (Syncing, r: ReadyForQuery) =>
      Complete(r, Some(Return(Response.ParseComplete(Response.Prepared(name)))))
    case (_, e: ErrorResponse) => Transition(Syncing, Respond(Throw(PgSqlServerError(e))))

    case (state, msg) => throw PgSqlNoSuchTransition("PrepareMachine", state, msg)
  }

}
