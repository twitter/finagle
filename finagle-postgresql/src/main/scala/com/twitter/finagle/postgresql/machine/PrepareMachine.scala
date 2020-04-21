package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.BackendMessage.ErrorResponse
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.postgresql.FrontendMessage.Describe
import com.twitter.finagle.postgresql.FrontendMessage.DescriptionTarget
import com.twitter.finagle.postgresql.FrontendMessage.Parse
import com.twitter.finagle.postgresql.FrontendMessage.Sync
import com.twitter.finagle.postgresql.PgSqlNoSuchTransition
import com.twitter.finagle.postgresql.PgSqlServerError
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.machine.StateMachine.Complete
import com.twitter.finagle.postgresql.machine.StateMachine.NoOp
import com.twitter.finagle.postgresql.machine.StateMachine.Respond
import com.twitter.finagle.postgresql.machine.StateMachine.SendSeveral
import com.twitter.finagle.postgresql.machine.StateMachine.Transition
import com.twitter.finagle.postgresql.machine.StateMachine.TransitionResult
import com.twitter.util.Return
import com.twitter.util.Throw

class PrepareMachine(name: Name, statement: String) extends StateMachine[Response.ParseComplete] {

  sealed trait State
  case object Parsing extends State
  case class Parsed(d: BackendMessage.ParameterDescription) extends State
  case object Syncing extends State

  override def start: TransitionResult[State, Response.ParseComplete] =
    Transition(
      Parsing,
      SendSeveral(
        Parse(name, statement, Nil),
        Describe(name, DescriptionTarget.PreparedStatement),
        Sync
      )
    )

  override def receive(state: State, msg: BackendMessage): TransitionResult[State, Response.ParseComplete] = (state, msg) match {
    case (Parsing, BackendMessage.ParseComplete) => Transition(Parsing, NoOp)
    case (Parsing, d: BackendMessage.ParameterDescription) => Transition(Parsed(d), NoOp)

    // NOTE: According to the documentation, because Bind hasn't been issued here,
    //   the format for the returned fields is unknown at this point.
    //   Because we issue a Describe on the portal, this incomplete information is not useful, so we ignore it.
    case (p: Parsed, _: BackendMessage.RowDescription) => Transition(p, NoOp)
    case (Parsed(desc), r: ReadyForQuery) =>
      Complete(r, Some(Return(Response.ParseComplete(Response.Prepared(name, desc.parameters)))))

    case (Syncing, r: ReadyForQuery) => Complete(r, None)
    case (_, e: ErrorResponse) => Transition(Syncing, Respond(Throw(PgSqlServerError(e))))

    case (state, msg) => throw PgSqlNoSuchTransition("PrepareMachine", state, msg)
  }

}
