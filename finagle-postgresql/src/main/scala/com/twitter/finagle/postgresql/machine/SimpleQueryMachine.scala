package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.BackendMessage.EmptyQueryResponse
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.postgresql.BackendResponse
import com.twitter.finagle.postgresql.FrontendMessage
import com.twitter.finagle.postgresql.Response
import com.twitter.util.Future

class SimpleQueryMachine(query: String) extends StateMachine[SimpleQueryMachine.State, Response] {

  import SimpleQueryMachine._

  override def start: StateMachine.TransitionResult[SimpleQueryMachine.State, Response] =
    StateMachine.TransitionAndSend(Init, FrontendMessage.Query(query))

  override def receive(state: SimpleQueryMachine.State, msg: BackendMessage): StateMachine.TransitionResult[SimpleQueryMachine.State, Response] = (state, msg) match {
    case (Init, EmptyQueryResponse) => StateMachine.Transition(Complete(BackendResponse(EmptyQueryResponse)))
    case (Complete(response), r: ReadyForQuery) => StateMachine.Complete(response, Future.value(r))
    case (state, msg) => sys.error(s"unhandled state $state event $msg")
  }
}

object SimpleQueryMachine {
  sealed trait State
  case object Init extends State
  case class Complete(r: Response) extends State
}
