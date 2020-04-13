package com.twitter.finagle

import com.twitter.finagle.postgresql.BackendMessage.CommandComplete
import com.twitter.finagle.postgresql.BackendMessage.EmptyQueryResponse
import com.twitter.finagle.postgresql.BackendMessage.Field
import com.twitter.finagle.postgresql.PgSqlServerError
import com.twitter.finagle.postgresql.Response.BackendResponse
import com.twitter.finagle.postgresql.Query
import com.twitter.finagle.postgresql.Response.ResultSet
import com.twitter.util.Throw

class SimpleQuerySpec extends PgSqlSpec with EmbeddedPgSqlSpec {

  "Simple Query" should {
    "return an empty result for an empty query" in {
      client(Query(""))
        .map { response =>
          response must beEqualTo(BackendResponse(EmptyQueryResponse))
        }
    }
    "return a server error for an invalid query" in {
      client(Query("invalid"))
        .liftToTry
        .map { response =>
          response.asScala must beFailedTry(beAnInstanceOf[PgSqlServerError])
          response match {
            case Throw(e: PgSqlServerError) =>
              e.field(Field.Code) must beSome("42601") // syntax_error
            case _ => ko
          }
        }
    }
    "return an CREATE ROLE command tag" in {
      client(Query("CREATE USER fake;"))
        .map {
          case BackendResponse(CommandComplete(commandTag)) => commandTag must_== "CREATE ROLE"
          case _ => ko
        }
    }
    "return a ResultSet for a SELECT query" in {
      client(Query("SELECT 1 AS one;"))
        .flatMap { response =>
          response must beAnInstanceOf[ResultSet]

          val rs@ResultSet(desc, _) = response
          desc.rowFields must haveSize(1)
          desc.rowFields.head.name must beEqualTo("one")

          rs.toSeq.map { rowSeq =>
            rowSeq must haveSize(1)
          }
        }
    }
  }

}
