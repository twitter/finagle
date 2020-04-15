package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.BackendMessage.DataRow
import com.twitter.finagle.postgresql.BackendMessage.EmptyQueryResponse
import com.twitter.finagle.postgresql.BackendMessage.RowDescription
import com.twitter.finagle.postgresql.FrontendMessage
import com.twitter.finagle.postgresql.PropertiesSpec
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.machine.StateMachine.Complete
import com.twitter.finagle.postgresql.machine.StateMachine.Respond
import com.twitter.finagle.postgresql.machine.StateMachine.Send
import com.twitter.finagle.postgresql.machine.StateMachine.Transition
import com.twitter.io.Reader
import org.scalacheck.Prop.forAll

class SimpleQueryMachineSpec extends MachineSpec[Response] with PropertiesSpec {

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

    def resultSetSpec(query: String, rowDesc: RowDescription, rows: List[DataRow]): Reader[DataRow] = {
      var rowReader: Option[Reader[DataRow]] = None

      val prep = List(
        checkQuery(query),
        receive(rowDesc),
        checkResult("responds") {
          case Transition(_, Respond(value)) =>
            value.asScala must beASuccessfulTry
            value.get must beLike {
              case Response.ResultSet(desc, reader) =>
                rowReader = Some(reader)
                desc must beEqualTo(rowDesc)
            }
        }
      )

      val sendRows = rows.map(receive)

      val post = List(
        receive(BackendMessage.CommandComplete("command tag")),
        receive(readyForQuery),
        checkCompletes
      )

      machineSpec(mkMachine(query))(
        prep ++ sendRows ++ post: _*
      )

      rowReader must beSome
      rowReader.get
    }

    "support empty result sets" in forAll { rowDesc: RowDescription =>
      Reader.toAsyncStream(resultSetSpec("select 1;", rowDesc, Nil)).toSeq.map { rows =>
        rows must beEmpty
      }
    }
    "return rows in order" in forAll { rs: TestResultSet =>
      Reader.toAsyncStream(resultSetSpec("select 1;", rs.desc, rs.rows)).toSeq.map { rows =>
        rows must beEqualTo(rows)
      }
    }
  }
}
