package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.Response.SimpleQueryResponse
import com.twitter.io.Reader

class SimpleQuerySpec extends PgSqlSpec with EmbeddedPgSqlSpec {

  "Simple Query" should {
    "multi-line" in {
      client(Query("create user fake;\nselect 1;"))
        .flatMap { response =>
          response must beAnInstanceOf[SimpleQueryResponse]
          val SimpleQueryResponse(stream) = response
            Reader.toAsyncStream(stream.flatMap {
              case Response.ResultSet(rowDescription, rows) => rows
              case e => Reader.value(e)
            }).toSeq
              .map { all =>
                println(all)
                true
              }
          }
        }
    }
    /*
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
  }*/

}
