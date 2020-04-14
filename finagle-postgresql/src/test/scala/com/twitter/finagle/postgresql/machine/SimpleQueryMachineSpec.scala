package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.BackendMessage.EmptyQueryResponse
import com.twitter.finagle.postgresql.FrontendMessage
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.machine.StateMachine.Complete
import com.twitter.finagle.postgresql.machine.StateMachine.Respond
import com.twitter.finagle.postgresql.machine.StateMachine.Send
import com.twitter.finagle.postgresql.machine.StateMachine.Transition
import org.scalacheck.Prop.forAll
import org.specs2.ScalaCheck

class SimpleQueryMachineSpec extends MachineSpec[Response] with ScalaCheck {

  def mkMachine(q: String): SimpleQueryMachine = new SimpleQueryMachine(q)

  val readyForQuery = BackendMessage.ReadyForQuery(BackendMessage.NoTx)

  def checkQuery(q: String) =
    checkResult("sends a query message") {
      case Transition(_, Send(FrontendMessage.Query(str))) =>
        str must_== q
    }

  def checkCompletes =
    checkResult("completes") {
      case Complete(ready, response) =>
        ready must beEqualTo(readyForQuery)
        response must beNone
    }

  "SimpleQueryMachine" should {

    "send the provided query string" in { query: String =>
      machineSpec(mkMachine(query)) {
        checkQuery(query)
      }
    }

    "support empty queries" in {
      machineSpec(mkMachine(""))(
        checkQuery(""),
        receive(BackendMessage.EmptyQueryResponse),
        checkResult("responds") {
          case Transition(_, Respond(value)) =>
            value.asScala must beASuccessfulTry.withValue(beEqualTo(Response.BackendResponse(EmptyQueryResponse)))
        },
        receive(readyForQuery),
        checkCompletes
      )
    }

    "support commands" in forAll { (command: String, commandTag: String) =>
      val commandComplete = BackendMessage.CommandComplete(commandTag)
      machineSpec(mkMachine(command))(
        checkQuery(command),
        receive(commandComplete),
        checkResult("responds") {
          case Transition(_, Respond(value)) =>
            value.asScala must beASuccessfulTry.withValue(beEqualTo(Response.BackendResponse(commandComplete)))
        },
        receive(readyForQuery),
        checkCompletes
      )
    }
  }
}
