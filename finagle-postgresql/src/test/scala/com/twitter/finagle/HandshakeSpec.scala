package com.twitter.finagle

import com.twitter.finagle.postgresql.BackendMessage
import com.twitter.finagle.postgresql.Response.BackendResponse
import com.twitter.finagle.postgresql.Sync

class HandshakeSpec extends PgSqlSpec with EmbeddedPgSqlSpec {

  "Handshake" should {
    "support password-less authentication" in {
      client(Sync)
        .map { response =>
          response must beEqualTo(BackendResponse(BackendMessage.ReadyForQuery(BackendMessage.NoTx)))
        }
    }
  }

}
