package com.twitter.finagle

import com.twitter.finagle.postgresql.BackendMessage.EmptyQueryResponse
import com.twitter.finagle.postgresql.Response.BackendResponse
import com.twitter.finagle.postgresql.Query

class SimpleQuerySpec extends PgSqlSpec with EmbeddedPgSqlSpec {

  "Simple Query" should {
    "return an empty result for an empty query" in {
      client(Query(""))
        .map { response =>
          response must beEqualTo(BackendResponse(EmptyQueryResponse))
        }
    }
  }

}
