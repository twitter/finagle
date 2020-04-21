package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.BackendMessage.Field
import com.twitter.finagle.postgresql.Response.SimpleQueryResponse
import com.twitter.io.Reader
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Throw
import org.specs2.matcher.MatchResult

class SimpleQuerySpec extends PgSqlSpec with EmbeddedPgSqlSpec {

  "Simple Query" should {

    def one(q: Request.Query)(check: Response.QueryResponse => Future[MatchResult[_]]) = {
      client(q)
        .flatMap {
          case r: SimpleQueryResponse => r.next.flatMap(check)
          case r => sys.error(s"unexpected response $r")
        }
    }

    "return an empty result for an empty query" in {
      one(Request.Query("")) {
        case Response.Empty => Future.value(ok)
        case r => sys.error(s"unexpected response $r")
      }
    }

    "return a server error for an invalid query" in {
      client(Request.Query("invalid"))
        .liftToTry
        .map { response =>
          response.asScala must beFailedTry(beAnInstanceOf[PgSqlServerError])
          response match {
            case Throw(e: PgSqlServerError) =>
              e.field(Field.Code) must beSome("42601") // syntax_error
            case r => sys.error(s"unexpected response $r")
          }
        }
    }
    "return an CREATE ROLE command tag" in {
      one(Request.Query("CREATE USER fake;")) {
        case Response.Command(tag) => Future.value(tag must_== "CREATE ROLE")
        case r => sys.error(s"unexpected response $r")
      }
    }
    "return a ResultSet for a SELECT query" in {
      one(Request.Query("SELECT 1 AS one;")) {
        case rs@Response.ResultSet(desc, _) =>
          desc must haveSize(1)
          desc.head.name must beEqualTo("one")
          rs.toSeq.map { rowSeq =>
            rowSeq must haveSize(1)
          }
        case r => sys.error(s"unexpected response $r")
      }
    }

    "multi-line" in {
      client(Request.Query("create user other;\nselect 1 as one;drop user other;"))
        .flatMap { response =>
          response must beAnInstanceOf[SimpleQueryResponse]
          val SimpleQueryResponse(stream) = response
          Reader.toAsyncStream(stream)
            .toSeq()
            .map { responses =>
              responses.toList must beLike {
                case first :: rs :: last :: Nil =>
                  first must beLike {
                    case Response.Command(tag) => tag must_== "CREATE ROLE"
                  }
                  rs must beLike {
                    case rs@Response.ResultSet(desc, _) =>
                      desc must haveSize(1)
                      desc.head.name must beEqualTo("one")
                      Await.result(rs.toSeq.map { rowSeq =>
                        rowSeq must haveSize(1)
                      })
                  }
                  last must beLike {
                    case Response.Command(tag) => tag must_== "DROP ROLE"
                  }
              }
            }
        }
    }
  }
}
