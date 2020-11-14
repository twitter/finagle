package com.twitter.finagle.postgresql

import com.twitter.finagle.Service
import com.twitter.finagle.postgresql.Types.Name
import com.twitter.finagle.postgresql.Types.WireValue
import com.twitter.io.Reader
import com.twitter.util.Future
import org.specs2.matcher.MatchResult

class PreparedStatementSpec extends PgSqlSpec with EmbeddedPgSqlSpec {

  // This query produces an infinite stream which is useful for testing cancellations and portal suspension
  val InfiniteResultSetQuery = """WITH RECURSIVE a(n) AS (
                                 |  SELECT 0
                                 | UNION ALL
                                 |  SELECT n+2 FROM a
                                 |)
                                 |SELECT * FROM a""".stripMargin

  "Prepared Statement" should {

    def prepareSpec(name: Name, s: String) =
      client(Request.Prepare(s, name))
        .map { response =>
          response must beLike {
            case Response.ParseComplete(prepared) => prepared.name must_== name
          }
        }

    def closingSpec(name: Name, s: String) =
      newClient(identity)().flatMap { svc =>
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

    def executeSpec(
      s: String,
      parameters: IndexedSeq[WireValue] = IndexedSeq.empty,
      maxResults: Int = 0
    )(f: (Service[Request, Response], Response) => Future[MatchResult[_]]) =
      newClient(identity)()
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

    def fullSpec(
      name: String,
      query: String,
      parameters: IndexedSeq[WireValue] = IndexedSeq.empty
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

    "support preparing select statements with one argument" in {
      prepareSpec(Name.Unnamed, "SELECT 1,$1")
    }

    fullSpec("select statements with no arguments", "CREATE TABLE test(col1 bigint)") {
      case Response.Command(tag) => Future(tag must beEqualTo("CREATE TABLE"))
      case _ => Future(ko)
    }

    "support preparing DML with one argument" in {
      withTmpTable { tableName =>
        prepareSpec(Name.Unnamed, s"UPDATE $tableName SET int_col = $$1")
      }
    }

    "support portal suspension" in {
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
