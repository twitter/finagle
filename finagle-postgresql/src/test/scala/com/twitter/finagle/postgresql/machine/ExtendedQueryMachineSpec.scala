package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.BackendMessage.BindComplete
import com.twitter.finagle.postgresql.BackendMessage.CommandComplete
import com.twitter.finagle.postgresql.BackendMessage.EmptyQueryResponse
import com.twitter.finagle.postgresql.BackendMessage.NoData
import com.twitter.finagle.postgresql.BackendMessage.NoTx
import com.twitter.finagle.postgresql.BackendMessage.ReadyForQuery
import com.twitter.finagle.postgresql.BackendMessage.RowDescription
import com.twitter.finagle.postgresql.FrontendMessage.Bind
import com.twitter.finagle.postgresql.FrontendMessage.Describe
import com.twitter.finagle.postgresql.FrontendMessage.DescriptionTarget
import com.twitter.finagle.postgresql.FrontendMessage.Execute
import com.twitter.finagle.postgresql.FrontendMessage.Flush
import com.twitter.finagle.postgresql.FrontendMessage.Sync
import com.twitter.finagle.postgresql.PgSqlServerError
import com.twitter.finagle.postgresql.PropertiesSpec
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.machine.StateMachine.Complete
import com.twitter.finagle.postgresql.machine.StateMachine.NoOp
import com.twitter.finagle.postgresql.machine.StateMachine.Respond
import com.twitter.finagle.postgresql.machine.StateMachine.Send
import com.twitter.finagle.postgresql.machine.StateMachine.SendSeveral
import com.twitter.finagle.postgresql.machine.StateMachine.Transition
import com.twitter.io.Buf
import com.twitter.util.Await
import com.twitter.util.Return
import com.twitter.util.Throw

class ExtendedQueryMachineSpec extends MachineSpec[Response.QueryResponse] with PropertiesSpec {

  def checkStartup(name: Name, parameters: IndexedSeq[Buf]): StepSpec =
    checkResult("start is several messages") {
      case Transition(_, SendSeveral(msgs)) =>
        msgs.toList must beLike {
          case a :: b :: c :: d :: Nil =>
            a must beEqualTo(Send(Bind(Name.Unnamed, name, Nil, Nil, Nil)))
            b must beEqualTo(Send(Describe(Name.Unnamed, DescriptionTarget.Portal)))
            c must beEqualTo(Send(Execute(Name.Unnamed, 0)))
            d must beEqualTo(Send(Flush))
        }
    }

  def checkNoOp(name: String): StepSpec =
    checkResult(name) {
      case Transition(_, NoOp) => ok
    }

  val handleSync = List(
    checkResult("sends Sync") {
      case Transition(_, Send(Sync)) => ok
    },
    receive(ReadyForQuery(NoTx))
  )
  // The ExtendedQueryMachine needs to send a Sync message to get the connection back to normal before completing
  val errorHandler: ErrorHandler = error => handleSync ++ defaultErrorHandler(error)

  def mkMachine(name: Name, parameters: IndexedSeq[Buf]): ExtendedQueryMachine =
    new ExtendedQueryMachine(name, parameters)

  "ExtendedQueryMachine" should {
    "send multiple messages on start" in prop { (name: Name, parameters: IndexedSeq[Buf]) =>
      machineSpec(mkMachine(name, parameters), errorHandler) {
        checkStartup(name, parameters)
      }
    }

    def baseSpec(name: Name, parameters: IndexedSeq[Buf], describeMessage: BackendMessage)(tail: StepSpec*) = {
      val head = List(
        checkStartup(name, parameters),
        receive(BindComplete),
        checkNoOp("handles BindComplete"),
        receive(describeMessage),
        checkNoOp("handles describe message"),
      )

      machineSpec(mkMachine(name, parameters), errorHandler)(head ++ tail: _*)
    }

    def nominalSpec(
      name: Name,
      parameters: IndexedSeq[Buf],
      describeMessage: BackendMessage,
      executeMessage: BackendMessage,
      expectedResponse: Response.QueryResponse,
    )= {
      baseSpec(name, parameters, describeMessage)(
        receive(executeMessage),
        checkResult("sends sync") {
          case Transition(_, Send(Sync)) => ok
        },
        receive(ReadyForQuery(NoTx)),
        checkResult("completes with expected response") {
          case Complete(_, Some(Return(response))) =>
            response must beEqualTo(expectedResponse)
        }
      )
    }

    "support empty queries" in prop { (name: Name, parameters: IndexedSeq[Buf], desc: RowDescription) =>
      nominalSpec(name, parameters, desc, EmptyQueryResponse, Response.Empty)
    }

    "support commands" in prop { (name: Name, parameters: IndexedSeq[Buf], commandTag: String) =>
      nominalSpec(name, parameters, NoData, CommandComplete(commandTag), Response.Command(commandTag))
    }

    "support result sets" in prop { (name: Name, parameters: IndexedSeq[Buf], rs: TestResultSet) =>
      rs.rows.nonEmpty ==> {
        var rowReader: Option[Response.ResultSet] = None
        val steps = rs.rows match {
          case Nil => sys.error("unexpected result set")
          case head :: tail =>
            List(
              receive(head),
              checkResult("responds") {
                case Transition(_, Respond(Return(r@Response.ResultSet(fields, _)))) =>
                  rowReader = Some(r)
                  fields must beEqualTo((rs.desc.rowFields))
              }
            ) ++ tail.map(receive(_))
        }
        val postSteps = List(
          receive(CommandComplete("TODO"))
        )
        baseSpec(name, parameters, rs.desc)(
          steps ++ postSteps: _*
        ) && {
          rowReader must beSome
          val rows = Await.result(rowReader.get.toSeq.liftToTry)
          // NOTE: this isn't as strict as it could be.
          //   Ideally we would only expect an error when one was injected
          rows must beLike {
            case Return(rows) => rows must beEqualTo(rs.rows)
            case Throw(PgSqlServerError(e)) => ok // injected error case
          }
        }
      }
    }

  }
}
