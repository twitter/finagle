package com.twitter.finagle

import com.twitter.finagle.postgresql.Messages
import com.twitter.finagle.postgresql.Messages.NoTx
import com.twitter.finagle.postgresql.Messages.ReadyForQuery

class HandshakeSpec extends PgSqlSpec with EmbeddedPgSqlSpec {

  "Handshake" should {
    "support password-less authentication" in {
      client(Messages.Sync)
        .map { response =>
          response must beEqualTo(ReadyForQuery(NoTx))
        }
    }
  }

}
