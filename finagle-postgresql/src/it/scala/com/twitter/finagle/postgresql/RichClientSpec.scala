package com.twitter.finagle.postgresql

import com.twitter.io.Buf
import com.twitter.io.Reader
import com.twitter.util.Await

class RichClientSpec extends PgSqlIntegrationSpec {

  "Rich client" should {

    "support multi-line queries" in withRichClient() { client =>
      Reader.toAsyncStream(client.multiQuery("select 1;select 2;"))
        .mapF(s => Client.Expect.ResultSet(s))
        .mapF(s => s.toSeq)
        .toSeq()
        .map { rs =>
          rs must haveSize(2)
        }
    }

    "read" in withRichClient() { client =>
      client
        .read("select 1;")
        .map(_.rows must haveSize(1))
    }

    "modify" in withRichClient() { client =>
      client
        .modify("create user fake;")
        .map(_ must beEqualTo(Response.Command("CREATE ROLE")))
    }

    "copy from" in withRichClient() { client =>
      withTmpTable() { tlbName =>
        client
          .modify(s"COPY $tlbName FROM STDIN;")
          .map(_ => ok)
      }
    }.pendingUntilFixed()

    "copy to" in withRichClient() { client =>
      withTmpTable() { tlbName =>
        client
          .modify(s"COPY $tlbName TO STDOUT;")
          .map(_ => ok)
      }
    }.pendingUntilFixed()

    "prepare read" in withRichClient() { client =>
      client
        .prepare("select 1")
        .read(Nil)
        .map(_.rows must haveSize(1))
    }

    "prepare modify" in withRichClient() { client =>
      client
        .prepare("create user another;")
        .modify(Nil)
        .map(_ must beEqualTo(Response.Command("CREATE ROLE")))
    }

    "prepare param" in withRichClient() { client =>
      client
        .prepare("select $1::bool, $2::bytea")
        .read(Parameter(true) :: Parameter(Buf.ByteArray(0, 1, 2, 3, 4)) :: Nil)
        .map { rs =>
          rs.rows must haveSize(1)
          rs.rows.head.get[Boolean](0) must beTrue
          rs.rows.head.get[Buf](1) must_== Buf.ByteArray(0, 1, 2, 3, 4)
        }
    }

    "prepare reuse" in withRichClient() { client =>
      val stmt = client.prepare("select $1::bool, $2::bytea")

      def read(param1: Boolean, param2: Buf) =
        stmt
          .read(Parameter(param1) :: Parameter(param2) :: Nil)
          .map { rs =>
            rs.rows must haveSize(1)
            rs.rows.head.get[Boolean](0) must_== param1
            rs.rows.head.get[Buf](1) must_== param2
          }

      Await.result(read(false, Buf.ByteArray(-1, 0, 1, 2)))
      Await.result(read(true, Buf.ByteArray(4, 3, 2, 1)))
    }
  }

}
