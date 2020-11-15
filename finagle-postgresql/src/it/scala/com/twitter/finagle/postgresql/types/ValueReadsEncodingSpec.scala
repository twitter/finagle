package com.twitter.finagle.postgresql.types

import java.nio.charset.StandardCharsets

import com.twitter.finagle.postgresql.PgSqlIntegrationSpec
import com.twitter.finagle.postgresql.PropertiesSpec
import com.twitter.finagle.postgresql.Request
import com.twitter.finagle.postgresql.Response
import com.twitter.util.Await

class ValueReadsEncodingSpec extends PgSqlIntegrationSpec with PropertiesSpec {

  val DbName = "iso_8859_1"
  def dbConnectionCfg = defaultConnectionCfg.copy(database = DbName)

  override def postgresContainerEnv = Map(
    "POSTGRES_INITDB_ARGS" -> "--encoding=LATIN1 --locale=C"
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    withStatement() { stmt =>
      val _ = stmt.execute(s"CREATE DATABASE $DbName ENCODING 'ISO88591';")
    }
    withStatement(dbConnectionCfg) { stmt =>
      stmt.execute("CREATE TABLE latin_encoded(v TEXT);")
      val _ = stmt.execute("INSERT INTO latin_encoded VALUES('é');")
    }
  }

  "ValueReads encoding" should {
    "detects encoding" in withService(dbConnectionCfg) { service =>
      service(Request.ConnectionParameters)
        .map {
          case p: Response.ConnectionParameters =>
            p.parsedParameters.serverEncoding must_== StandardCharsets.ISO_8859_1
            p.parsedParameters.clientEncoding must_== StandardCharsets.ISO_8859_1
          case _ => sys.error("invalid response")
        }
    }

    "support changing encoding" in withService(dbConnectionCfg) { service =>
      Await.result(service(Request.Query("SET client_encoding = 'UTF8';")))
      service(Request.ConnectionParameters)
        .map {
          case p: Response.ConnectionParameters =>
            p.parsedParameters.serverEncoding must_== StandardCharsets.ISO_8859_1
            p.parsedParameters.clientEncoding must_== StandardCharsets.UTF_8
          case _ => sys.error("invalid response")
        }
    }.pendingUntilFixed("Receiving ParameterStatus messages mid-connection is not yet supported")

    "use appropriate encoding when reading text" in withRichClient(dbConnectionCfg) { client =>
      client.select("select * from latin_encoded;")(_.get[String](0))
        .map { values =>
          values must_== List("é")
        }
    }
  }
}
