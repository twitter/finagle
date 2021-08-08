package com.twitter.finagle.postgresql.machine

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.BackendMessage.CommandTag
import com.twitter.finagle.postgresql.BackendMessage.DataRow
import com.twitter.finagle.postgresql.BackendMessage.RowDescription
import com.twitter.finagle.postgresql.FrontendMessage
import com.twitter.finagle.postgresql.PgSqlNoSuchTransition
import com.twitter.finagle.postgresql.PgSqlServerError
import com.twitter.finagle.postgresql.PropertiesSpec
import com.twitter.finagle.postgresql.Response
import com.twitter.finagle.postgresql.Response.{ConnectionParameters, QueryResponse, Row}
import com.twitter.finagle.postgresql.machine.StateMachine.Complete
import com.twitter.finagle.postgresql.machine.StateMachine.Respond
import com.twitter.finagle.postgresql.machine.StateMachine.Send
import com.twitter.finagle.postgresql.machine.StateMachine.Transition
import com.twitter.io.Reader
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Return
import com.twitter.util.Throw
import com.twitter.util.Try
import org.scalatest.Assertion

class SimpleQueryMachineSpec extends MachineSpec[Response] with PropertiesSpec {

  def mkMachine(q: String): SimpleQueryMachine =
    new SimpleQueryMachine(q, ConnectionParameters.empty)

  val readyForQuery = BackendMessage.ReadyForQuery(BackendMessage.NoTx)

  def checkQuery(q: String) =
    checkResult("sends a query message") {
      case Transition(_, Send(FrontendMessage.Query(str))) =>
        str must be(q)
    }

  def checkCompletes =
    checkResult("completes") {
      case Complete(ready, response) =>
        ready must be(readyForQuery)
        response mustBe empty
    }
  type QueryResponseCheck = PartialFunction[Try[Response.QueryResponse], Assertion]

  def checkSingleResponse(f: QueryResponseCheck) =
    checkResult("captures one response") {
      case Transition(_, Respond(value)) =>
        value.asScala must beSuccessfulTry {
          beLike[Response] {
            case r @ Response.SimpleQueryResponse(_) =>
              Await.result(r.next.liftToTry) must beLike(f)
          }
        }
    }

  def multiQuerySpec(
    query: String,
    first: (BackendMessage, QueryResponseCheck),
    others: (BackendMessage, QueryResponseCheck)*
  ) = {

    var sqr: Option[Response.SimpleQueryResponse] = None

    val (msg, firstCheck) = first
    val firstSteps = List(
      receive(msg),
      checkResult("responds") {
        case Transition(_, Respond(value)) =>
          value.asScala must beSuccessfulTry {
            beLike[Response] {
              case r: Response.SimpleQueryResponse =>
                sqr = Some(r)
                succeed
            }
          }
      }
    )

    val steps = checkQuery(query) :: firstSteps ++
      others.map { case (msg, _) => receive(msg) } ++
      List(
        receive(readyForQuery),
        checkCompletes
      )

    machineSpec(mkMachine(query))(steps: _*)

    sqr match {
      case None => Future.value(fail() :: Nil)
      case Some(s) =>
        Reader
          .toAsyncStream(s.responses)
          .toSeq()
          .map { actual =>
            (actual zip (firstCheck :: others.map(_._2).toList))
              .map {
                case (a, check) =>
                  check(Return(a))
              }
          }
    }
  }

  def singleQuerySpec(
    query: String,
    msg: BackendMessage
  )(
    f: PartialFunction[Try[Response.QueryResponse], Assertion]
  ) =
    multiQuerySpec(query, msg -> f)

  "SimpleQueryMachine" should {

    "send the provided query string" in prop { query: String =>
      machineSpec(mkMachine(query)) {
        checkQuery(query)
      }
    }

    "support empty queries" in {
      singleQuerySpec("", BackendMessage.EmptyQueryResponse) {
        case Return(value) => value must be(Response.Empty)
      }
    }

    "support commands" in prop { (command: String, commandTag: CommandTag) =>
      singleQuerySpec(command, BackendMessage.CommandComplete(commandTag)) {
        case Return(value) => value must be(Response.Command(commandTag))
      }
    }

    def resultSetSpec(
      query: String,
      rowDesc: RowDescription,
      rows: List[DataRow]
    )(
      f: Seq[Row] => Assertion
    ) = {
      var rowReader: Option[Response.ResultSet] = None

      val prep = List(
        checkQuery(query),
        receive(rowDesc),
        checkSingleResponse {
          case Return(value) =>
            value must beLike[QueryResponse] {
              case rs @ Response.ResultSet(desc, _, _) =>
                rowReader = Some(rs)
                desc must be(rowDesc.rowFields)
            }
        }
      )

      val sendRows = rows.map(receive(_))

      val post = List(
        receive(
          BackendMessage.CommandComplete(CommandTag.AffectedRows(CommandTag.Select, rows.size))),
        receive(readyForQuery),
        checkCompletes
      )

      oneMachineSpec(mkMachine(query))(prep ++ sendRows ++ post: _*)
      rowReader mustBe defined
      rowReader.get.toSeq.map(f)

      rowReader = None
      // NOTE: machineErrorSpec returns a Prop which we combine with another using &&
      //   It's kind of weird, but specs2 isn't really helping here.
      machineErrorSpec(mkMachine(query))(prep ++ sendRows ++ post: _*)

      // NOTE: the randomization of the error makes it possible that:
      //   * we read no rows at all
      //   * we read all rows (and the error isn't surfaced)
      //   * we read partial rows and then an exception
      rowReader match {
        case None => succeed
        case Some(r) =>
          rowReader = None // TODO: the statefulness of the test is pretty brittle
          r.toSeq.liftToTry.map {
            case Return(rows) =>
              f(rows) // if we read all rows, then we should check that they're what we expect
            case Throw(t) => t mustBe an[PgSqlServerError] // the error should surface here
          }
      }

    }

    "support empty result sets" in prop { rowDesc: RowDescription =>
      resultSetSpec("select 1;", rowDesc, Nil) { rows =>
        rows mustBe empty
      }
    }

    "return rows in order" in prop { rs: TestResultSet =>
      resultSetSpec("select 1;", rs.desc, rs.rows) { rows =>
        rows must be(rs.rows.map(_.values))
      }
    }

    "support multiline queries" in {
      (command: String, firstTag: CommandTag, secondTag: CommandTag) =>
        multiQuerySpec(
          command,
          BackendMessage.CommandComplete(firstTag) -> {
            case Return(value) =>
              value must be(Response.Command(firstTag))
          },
          BackendMessage.EmptyQueryResponse -> {
            case Return(value) => value must be(Response.Empty)
          },
          BackendMessage.CommandComplete(secondTag) -> {
            case Return(value) =>
              value must be(Response.Command(secondTag))
          }
        )
    }

    "fail when no transition exist" in {
      val machine = mkMachine("bogus")
      an[PgSqlNoSuchTransition] shouldBe thrownBy {
        machine.receive(machine.Sent, BackendMessage.PortalSuspended)
      }
    }
  }
}
