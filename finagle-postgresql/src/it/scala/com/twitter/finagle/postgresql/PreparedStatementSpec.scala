package com.twitter.finagle.postgresql

import java.nio.charset.StandardCharsets

import com.twitter.finagle.Service
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.finagle.postgresql.types.PgType
import com.twitter.finagle.postgresql.types.ValueWrites
import com.twitter.io.Reader
import com.twitter.util.Future
import org.specs2.matcher.MatchResult

class PreparedStatementSpec extends PgSqlIntegrationSpec {

  // This query produces an infinite stream which is useful for testing cancellations and portal suspension
  val InfiniteResultSetQuery = """WITH RECURSIVE a(n) AS (
                                 |  SELECT 0
                                 | UNION ALL
                                 |  SELECT n+2 FROM a
                                 |)
                                 |SELECT * FROM a""".stripMargin

  def write[T](tpe: PgType, value: T)(implicit twrites: ValueWrites[T]): WireValue =
    twrites.writes(tpe, value, StandardCharsets.UTF_8)

  "Prepared Statement" should {

    def prepareSpec(name: Name, s: String) = withService() { client =>
      client(Request.Prepare(s, name))
        .map { response =>
          response must beLike {
            case Response.ParseComplete(prepared) => prepared.name must_== name
          }
        }
    }

    def closingSpec(name: Name, s: String) = withClient() { client =>
      client().flatMap { svc =>
        svc(Request.Prepare(s, name))
          .map { response =>
            response must beLike {
              case Response.ParseComplete(_) => ok
            }
          }
          .flatMap { _ =>
            svc(Request.CloseStatement(name))
              .map { response =>
                response must beLike {
                  case Response.Ready => ok
                }
              }
          }
          .respond { _ =>
            val _ = svc.close()
          }
      }
    }

    def executeSpec(
      s: String,
      parameters: Seq[WireValue] = Seq.empty,
      maxResults: Int = 0
    )(f: (Service[Request, Response], Response) => Future[MatchResult[_]]) = withClient() { client =>
      client()
        .flatMap { client =>
          client(Request.Prepare(s))
            .flatMap {
              case Response.ParseComplete(prepared) =>
                client(Request.ExecutePortal(prepared, parameters, maxResults = maxResults))
                  .flatMap { response =>
                    f(client, response)
                  }
              case _ => Future(ko)
            }
        }
    }

    def fullSpec(
      name: String,
      query: => String,
      parameters: Seq[WireValue] = Seq.empty
    )(f: Response => Future[MatchResult[_]]) =
      fragments(
        List(
          s"support preparing unnamed prepared $name" in {
            prepareSpec(Name.Unnamed, query)
          },
          s"support preparing named prepared $name" in {
            prepareSpec(Name.Named(query), query)
          },
          s"support closing unnamed prepared $name" in {
            closingSpec(Name.Unnamed, query)
          },
          s"support closing named prepared $name" in {
            closingSpec(Name.Named(query), query)
          },
          s"support executing $name" in {
            executeSpec(query, parameters) { case (_, response) => f(response) }
          }
        )
      )

    fullSpec("select statements with no arguments", "select 1") {
      case Response.ResultSet(_, rows, _) => Reader.toAsyncStream(rows).toSeq().map(r => r must haveSize(1))
      case _ => Future(ko)
    }

    fullSpec("select statements with one argument", "SELECT 1, $1::bool", write(PgType.Bool, true) :: Nil) {
      case rs: Response.ResultSet =>
        rs.toSeq.map { rows =>
          rows must haveSize(1)
          rows.head must haveSize(2)
        }
      case _ => Future(ko)
    }

    fullSpec("select statements with no arguments", "CREATE TABLE test(col1 bigint)") {
      case Response.Command(tag) => Future(tag must beEqualTo("CREATE TABLE"))
      case _ => Future(ko)
    }

    // This is a hack to have a temp table to work with in the following spec.
    lazy val tableName = withTmpTable()(identity)
    fullSpec("DML with one argument", s"INSERT INTO $tableName(int_col) VALUES($$1)", write(PgType.Int4, 56) :: Nil) {
      case Response.Command(tag) => Future(tag must beEqualTo("INSERT 0 1"))
      case _ => Future(ko)
    }

    // TODO: investigate CRDB failure
    "support portal suspension" in backend(Postgres) {
      val firstBatchSize = 17
      val secondBatchSize = 143
      executeSpec(InfiniteResultSetQuery, maxResults = firstBatchSize) {
        case (client, rs @ Response.ResultSet(_, _, _)) =>
          rs.toSeq
            .flatMap { batch =>
              batch must haveSize(firstBatchSize)
              client(Request.ResumePortal(Name.Unnamed, maxResults = secondBatchSize))
                .flatMap {
                  case rs @ Response.ResultSet(_, _, _) =>
                    rs.toSeq.map(_ must haveSize(secondBatchSize))
                  case _ => Future.value(ko)
                }
            }
        case _ => Future.value(ko)
      }
    }
  }
}
