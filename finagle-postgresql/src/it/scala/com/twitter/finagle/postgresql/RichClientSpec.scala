package com.twitter.finagle.postgresql

import com.twitter.io.Reader

class RichClientSpec extends PgSqlSpec with EmbeddedPgSqlSpec {

  "Rich client" should {

    "support multi-line queries" in {
      newRichClient
        .multiQuery("select 1;select 2;")
        .flatMap { statements =>
          Reader.toAsyncStream(statements)
            .mapF(s => Client.Expect.ResultSet(s))
            .mapF(s => s.toSeq)
            .toSeq
        }
        .map { rs =>
          rs must haveSize(2)
        }
    }

    "read" in {
      newRichClient
        .read("select 1;")
        .map(_.rows must haveSize(1))
    }

    "modify" in {
      newRichClient
        .modify("create user fake;")
        .map(_ must beEqualTo(Response.Command("CREATE ROLE")))
    }

    "prepare read" in {
      newRichClient
        .prepare("select 1")
        .read(Nil)
        .map(_.rows must haveSize(1))
    }

    "prepare modify" in {
      newRichClient
        .prepare("create user another;")
        .modify(Nil)
        .map(_ must beEqualTo(Response.Command("CREATE ROLE")))
    }
  }

}
