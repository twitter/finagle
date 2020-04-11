package com.twitter.finagle

import com.twitter.finagle.postgresql.Messages
import com.twitter.finagle.postgresql.Messages.NoTx
import com.twitter.finagle.postgresql.Messages.ReadyForQuery
import com.twitter.util.Await
import org.specs2.mutable.Specification

class HandshakeSpec extends Specification with EmbeddedPgSqlSpec {

  "Handshake" should {
    "support password-less authentication" in {
      Await.result(client(Messages.Sync)) must beEqualTo(ReadyForQuery(NoTx))
    }
  }

}
