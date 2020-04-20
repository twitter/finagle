package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.Response.Prepared
import com.twitter.io.Reader
import com.twitter.util.Future

class PreparedStatementSpec extends PgSqlSpec with EmbeddedPgSqlSpec {

  "Prepared Statement" should {
    "work" in { // TODO: this is just for testing, the actual API would be different
      newClient(identity).apply().flatMap { client =>
        def execute(prepared: Prepared) = {
          client(Request.Execute(prepared, IndexedSeq.empty)).flatMap {
            case Response.ResultSet(desc, rows) =>
              Reader.toAsyncStream(rows).toSeq.map(r => r must haveSize(1))
            case _ => Future.exception(new IllegalStateException())
          }
        }


        client(Request.Prepare("select 1"))
          .flatMap {
            case Response.ParseComplete(prepared) =>
              (execute(prepared) join execute(prepared))
                .map { case(a,b) =>
                  a and b
                }
            case _ => Future.exception(new IllegalStateException())
          }
      }
    }
  }
}
