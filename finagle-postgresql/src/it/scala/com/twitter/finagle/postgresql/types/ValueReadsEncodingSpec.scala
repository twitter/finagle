package com.twitter.finagle.postgresql.types

import java.nio.charset.StandardCharsets

import com.twitter.finagle.postgresql.Client
import com.twitter.finagle.postgresql.EmbeddedPgSqlSpec
import com.twitter.finagle.postgresql.PgSqlSpec
import com.twitter.finagle.postgresql.PropertiesSpec
import com.twitter.finagle.postgresql.Request
import com.twitter.finagle.postgresql.Response
import com.twitter.util.Await
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres

class ValueReadsEncodingSpec extends PgSqlSpec with EmbeddedPgSqlSpec with PropertiesSpec {

  override def configure(b: EmbeddedPostgres.Builder): EmbeddedPostgres.Builder =
    b.setLocaleConfig("encoding", "LATIN1")
      .setLocaleConfig("locale", "C")

  val DbName = "iso_8859_1"

  override def prep(e: EmbeddedPostgres): EmbeddedPostgres = {
    using(e.getPostgresDatabase.getConnection) { conn =>
      using(conn.createStatement()) { stmt =>
        stmt.execute(s"CREATE DATABASE $DbName ENCODING 'ISO88591';")
      }
    }
    using(e.getDatabase("postgres", DbName).getConnection) { conn =>
      using(conn.createStatement()) { stmt =>
        stmt.execute("CREATE TABLE latin_encoded(v TEXT);")
        stmt.execute("INSERT INTO latin_encoded VALUES('é');")
      }
    }
    e
  }

  "ValueReads encoding" should {
    "detects encoding" in {
      val service = Await.result(newClient(_.withDatabase(DbName)).apply())
      service(Request.ConnectionParameters)
        .map {
          case p: Response.ConnectionParameters =>
            p.parsedParameters.serverEncoding must_== StandardCharsets.ISO_8859_1
            p.parsedParameters.clientEncoding must_== StandardCharsets.ISO_8859_1
          case _ => sys.error("invalid response")
        }
    }

    "support changing encoding" in {
      val service = Await.result(newClient(_.withDatabase(DbName)).apply())
      Await.result(service(Request.Query("SET client_encoding = 'UTF8';")))
      service(Request.ConnectionParameters)
        .map {
          case p: Response.ConnectionParameters =>
            p.parsedParameters.serverEncoding must_== StandardCharsets.ISO_8859_1
            p.parsedParameters.clientEncoding must_== StandardCharsets.UTF_8
          case _ => sys.error("invalid response")
        }
    }.pendingUntilFixed("Receiving ParameterStatus messages mid-connection is not yet supported")

    "use appropriate encoding when reading text" in {
      val client = Client(newClient(_.withDatabase(DbName)))
      client.select("select * from latin_encoded;")(_.get[String](0))
        .map { values =>
          values must_== List("é")
        }
    }
  }
}
