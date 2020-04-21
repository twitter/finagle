package com.twitter.finagle.postgresql

import com.twitter.io.Buf
import com.twitter.io.Reader
import com.twitter.util.Future
import org.specs2.matcher.MatchResult

class PreparedStatementSpec extends PgSqlSpec with EmbeddedPgSqlSpec {

  "Prepared Statement" should {

    def prepareSpec(s: String) =
      client(Request.Prepare(s))
        .map { response =>
          response must beLike {
            case Response.ParseComplete(_) => ok
          }
        }

    def executeSpec(s: String, parameters: IndexedSeq[Buf] = IndexedSeq.empty)(f: Response => Future[MatchResult[_]]) =
      newClient(identity)()
        .flatMap { client =>
          client(Request.Prepare(s))
            .flatMap {
              case Response.ParseComplete(prepared) =>
                client(Request.Execute(prepared, parameters))
                  .flatMap { response =>
                    f(response)
                  }
              case _ => Future(ko)
            }
        }

    def fullSpec(name: String, query: String, parameters: IndexedSeq[Buf] = IndexedSeq.empty)(f: Response => Future[MatchResult[_]]) =
      fragments(
        List(
          s"support preparing $name" in {
            prepareSpec(query)
          },
          s"support executing $name" in {
            executeSpec(query, parameters)(f)
          }
        )
      )

    fullSpec("select statements with no arguments", "select 1") {
      case Response.ResultSet(_, rows) => Reader.toAsyncStream(rows).toSeq.map(r => r must haveSize(1))
      case _ => Future(ko)
    }

    "support preparing select statements with one argument" in {
      prepareSpec("SELECT 1,$1")
    }

    fullSpec("select statements with no arguments", "CREATE TABLE test(col1 bigint)") {
      case Response.Command(tag) => Future(tag must beEqualTo("CREATE TABLE"))
      case _ => Future(ko)
    }

    "support preparing DML with one argument" in {
      withTmpTable { tableName =>
        prepareSpec(s"UPDATE $tableName SET int_col = $$1")
      }
    }
  }
}
