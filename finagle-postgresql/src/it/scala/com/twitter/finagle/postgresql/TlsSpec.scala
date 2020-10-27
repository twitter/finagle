package com.twitter.finagle.postgresql

import com.twitter.finagle.PostgreSql
import com.twitter.finagle.ssl.TrustCredentials
import com.twitter.finagle.ssl.client.SslClientConfiguration
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres

class TlsSpec extends PgSqlSpec with EmbeddedPgSqlEach with ResourceFileSpec {

  def configureTls(builder: EmbeddedPostgres.Builder): EmbeddedPostgres.Builder =
    builder
      .setServerConfig("ssl", "true")
      .setServerConfig("ssl_cert_file", toTmpFile("/server.crt").getAbsolutePath)
      .setServerConfig("ssl_key_file", toTmpFile("/server.key").getAbsolutePath)

  def withTls(client: PostgreSql.Client): PostgreSql.Client =
    client.withTransport.tls(SslClientConfiguration(trustCredentials = TrustCredentials.Insecure))

  "TLS" should {

    "support tls" in embeddedPsql(configureTls, withTls) { (_, client) =>
      client.toService(Request.Sync)
        .map { response =>
          response must beEqualTo(Response.Ready)
        }
    }

    "handle unsupported tls" in embeddedPsql(identity, withTls) { (_, client) =>
      client.toService(Request.Sync)
        .liftToTry
        .map { response =>
          response.asScala must beAFailedTry(beEqualTo(PgSqlTlsUnsupportedError))
        }
    }
  }
}
