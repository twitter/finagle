package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.BackendMessage._
import com.twitter.finagle.postgresql.FrontendMessage._
import com.twitter.finagle.postgresql.Response.ConnectionParameters
import com.twitter.finagle.postgresql.Response.Prepared
import com.twitter.finagle.postgresql.Types.Format
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.finagle.postgresql._
import com.twitter.finagle.postgresql.machine.StateMachine._
import com.twitter.util.Await
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try
import org.scalatestplus.scalacheck.Checkers

class ExecuteMachineSpec
    extends MachineSpec[Response.QueryResponse]
    with PropertiesSpec
    with Checkers {

  def checkStartup(name: Name, portalName: Name, parameters: IndexedSeq[WireValue]): StepSpec =
    checkResult("start is several messages") {
      case Transition(_, SendSeveral(msgs @ _*)) =>
        msgs.toList must beLike[Seq[FrontendMessage]] {
          case a :: b :: c :: d :: Nil =>
            a must be(
              Bind(portalName, name, Format.Binary :: Nil, parameters, Format.Binary :: Nil))
            b must be(Describe(portalName, DescriptionTarget.Portal))
            c must be(Execute(portalName, 0))
            d must be(Flush)
        }
    }

  def checkNoOp(name: String): StepSpec =
    checkResult(name) {
      case Transition(_, NoOp) => succeed
    }

  val handleSync = List(
    checkResult("sends Sync") {
      case Transition(_, Send(Sync)) => succeed
    },
    receive(ReadyForQuery(NoTx))
  )
  // The ExecuteMachine needs to send a Sync message to get the connection back to normal before completing
  val errorHandler: ErrorHandler = error => handleSync ++ defaultErrorHandler(error)

  def mkMachine(name: Name, portalName: Name, parameters: IndexedSeq[WireValue]): ExecuteMachine =
    new ExecuteMachine(
      req = Request.ExecutePortal(Prepared(name, IndexedSeq.empty), parameters, portalName),
      parameters = ConnectionParameters.empty,
      () => ()
    )

  "ExecuteMachine" should {
    "send multiple messages on start" in prop {
      (name: Name, portalName: Name, parameters: IndexedSeq[WireValue]) =>
        machineSpec(mkMachine(name, portalName, parameters), errorHandler) {
          checkStartup(name, portalName, parameters)
        }
    }

    def baseSpec(
      name: Name,
      portalName: Name,
      parameters: IndexedSeq[WireValue],
      describeMessage: BackendMessage
    )(
      tail: StepSpec*
    ) = {
      val head = List(
        checkStartup(name, portalName, parameters),
        receive(BindComplete),
        checkNoOp("handles BindComplete"),
        receive(describeMessage),
        checkNoOp("handles describe message"),
      )

      machineSpec(mkMachine(name, portalName, parameters), errorHandler)(head ++ tail: _*)
    }

    def nominalSpec(
      name: Name,
      portalName: Name,
      parameters: IndexedSeq[WireValue],
      describeMessage: BackendMessage,
      executeMessage: BackendMessage,
      expectedResponse: Response.QueryResponse,
    ) =
      baseSpec(name, portalName, parameters, describeMessage)(
        receive(executeMessage),
        checkResult("sends sync") {
          case Transition(_, Send(Sync)) => succeed
        },
        receive(ReadyForQuery(NoTx)),
        checkResult("completes with expected response") {
          case Complete(_, Some(Return(response))) =>
            response must be(expectedResponse)
        }
      )

    "support empty queries" in prop {
      (name: Name, portalName: Name, parameters: IndexedSeq[WireValue], desc: RowDescription) =>
        nominalSpec(name, portalName, parameters, desc, EmptyQueryResponse, Response.Empty)
    }

    "support queries returning 0 rows" in prop {
      (name: Name, portalName: Name, parameters: IndexedSeq[WireValue], desc: RowDescription) =>
        nominalSpec(
          name,
          portalName,
          parameters,
          desc,
          CommandComplete(CommandTag.AffectedRows(BackendMessage.CommandTag.Select, 0)),
          Response.Empty)
    }

    "support commands" in prop {
      (name: Name, portalName: Name, parameters: IndexedSeq[WireValue], commandTag: CommandTag) =>
        nominalSpec(
          name,
          portalName,
          parameters,
          NoData,
          CommandComplete(commandTag),
          Response.Command(commandTag))
    }

    "support result sets" in prop {
      (name: Name, portalName: Name, parameters: IndexedSeq[WireValue], rs: TestResultSet) =>
        whenever(rs.rows.nonEmpty) {
          var rowReader: Option[Response.ResultSet] = None
          val steps = rs.rows match {
            case Nil => sys.error("unexpected result set")
            case head :: tail =>
              List(
                receive(head),
                checkResult("responds") {
                  case Transition(_, Respond(Return(r @ Response.ResultSet(fields, _, _)))) =>
                    rowReader = Some(r)
                    fields must be(rs.desc.rowFields)
                }
              ) ++ tail.map(receive(_))
          }
          val postSteps = List(
            receive(CommandComplete(CommandTag.AffectedRows(CommandTag.Select, rs.rows.size)))
          )
          baseSpec(name, portalName, parameters, rs.desc)(
            steps ++ postSteps: _*
          )

          rowReader mustBe defined
          val rows = Await.result(rowReader.get.toSeq.liftToTry)
          // NOTE: this isn't as strict as it could be.
          //   Ideally we would only expect an error when one was injected
          rows must beLike[Try[Seq[Response.Row]]] {
            case Return(rows) => rows must be(rs.rows.map(_.values))
            case Throw(PgSqlServerError(_)) => succeed // injected error case
          }
        }
    }

    "support result sets with notice messages" in prop {
      (name: Name, portalName: Name, parameters: IndexedSeq[WireValue], rs: TestResultSet) =>
        whenever(rs.rows.nonEmpty) {
          var rowReader: Option[Response.ResultSet] = None
          val steps = rs.rows match {
            case Nil => sys.error("unexpected result set")
            case head :: tail =>
              List(
                receive(head),
                checkResult("responds") {
                  case Transition(_, Respond(Return(r @ Response.ResultSet(fields, _, _)))) =>
                    rowReader = Some(r)
                    fields must be(rs.desc.rowFields)
                },
                receive(NoticeResponse(Map.empty)),
              ) ++ tail.map(receive(_))
          }
          val postSteps = List(
            receive(CommandComplete(CommandTag.AffectedRows(CommandTag.Select, rs.rows.size)))
          )
          val preSteps = List(
            receive(NoticeResponse(Map.empty))
          )

          baseSpec(name, portalName, parameters, rs.desc)(
            preSteps ++ steps ++ postSteps: _*
          )

          rowReader mustBe defined
          val rows = Await.result(rowReader.get.toSeq.liftToTry)
          // NOTE: this isn't as strict as it could be.
          //   Ideally we would only expect an error when one was injected
          rows must beLike[Try[Seq[Response.Row]]] {
            case Return(rows) => rows must be(rs.rows.map(_.values))
            case Throw(PgSqlServerError(_)) => succeed // injected error case
          }
        }
    }

    "fail when no transition exist" in {
      val machine = mkMachine(Name.Unnamed, Name.Unnamed, IndexedSeq.empty)
      an[PgSqlNoSuchTransition] must be thrownBy machine.receive(
        ExecuteMachine.Binding,
        BackendMessage.PortalSuspended)
    }
  }
}
