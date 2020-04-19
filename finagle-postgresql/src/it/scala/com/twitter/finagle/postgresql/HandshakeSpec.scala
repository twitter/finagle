package com.twitter.finagle.postgresql

import com.twitter.finagle.postgresql.Response.BackendResponse

class HandshakeSpec extends PgSqlSpec with EmbeddedPgSqlSpec {

  "Handshake" should {
    "support password-less authentication" in {
      client(Request.Sync)
        .map { response =>
          response must beEqualTo(BackendResponse(BackendMessage.ReadyForQuery(BackendMessage.NoTx)))
        }
    }
  }

}
