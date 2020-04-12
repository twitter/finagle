package com.twitter.finagle

import com.twitter.finagle.postgresql.BackendResponse
import com.twitter.finagle.postgresql.Messages.NoTx
import com.twitter.finagle.postgresql.Messages.ReadyForQuery
import com.twitter.finagle.postgresql.Sync

class HandshakeSpec extends PgSqlSpec with EmbeddedPgSqlSpec {

  "Handshake" should {
    "support password-less authentication" in {
      client(Sync)
        .map { response =>
          response must beEqualTo(BackendResponse(ReadyForQuery(NoTx)))
        }
    }
  }

}
