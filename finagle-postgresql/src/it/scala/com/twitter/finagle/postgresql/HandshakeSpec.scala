package com.twitter.finagle.postgresql

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres

class HandshakeSpec extends PgSqlSpec with EmbeddedPgSqlSpec with ResourceFileSpec {

  override def configure(builder: EmbeddedPostgres.Builder): EmbeddedPostgres.Builder =
    builder.setServerConfig("hba_file", toTmpFile("/handshake_pg_hba.conf").getAbsolutePath)

  override def prep(e: EmbeddedPostgres): EmbeddedPostgres = {
    using(e.getPostgresDatabase.getConnection(TestDbUser, "")) { conn =>
      using(conn.createStatement()) { stmt =>
        stmt.execute("CREATE USER clear_user WITH PASSWORD 'not secret'")
        stmt.execute("CREATE USER md5_user WITH PASSWORD 'super secret'")
      }
    }
    e
  }

  "Handshake" should {

    def authSpec(username: String, password: Option[String]) =
      newClient(_.withCredentials(username, password))
        .toService(Request.ConnectionParameters)
        .map { response =>
          response must beLike {
            case params: Response.ConnectionParameters =>
              params.parameterMap(BackendMessage.Parameter.SessionAuthorization) must_== username
          }
        }

    "support password-less authentication" in {
      authSpec("postgres", None)
    }

    "support clear text authentication" in {
      authSpec("clear_user", Some("not secret"))
    }

    "support md5 authentication" in {
      authSpec("md5_user", Some("super secret"))
    }
  }

}
