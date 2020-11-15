package com.twitter.finagle.postgresql

import com.spotify.docker.client.messages.HostConfig
import com.twitter.finagle.PostgreSql
import com.twitter.finagle.ssl.TrustCredentials
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.whisk.docker.testkit.ContainerSpec

class TlsSpec extends PgSqlIntegrationSpec with ResourceFileSpec {

  override def configure(spec: ContainerSpec): ContainerSpec = {
    spec
      .withVolumeBindings(
        HostConfig.Bind.builder()
          .from(toTmpFile("/server.crt").getAbsolutePath)
          .to("/certs/server.crt")
          .readOnly(true)
          .build(),
        HostConfig.Bind.builder()
          .from(toTmpFile("/server.key").getAbsolutePath)
          .to("/certs/server.key")
          .readOnly(true)
          .build()
      )
      .withCommand(
        "-c", "ssl=true",
        "-c", "ssl_cert_file=/certs/server.crt",
        "-c", "ssl_key_file=/certs/server.key",
      )
  }

  def withTls(client: PostgreSql.Client): PostgreSql.Client =
    client.withTransport.tls(SslClientConfiguration(trustCredentials = TrustCredentials.Insecure))

  "TLS" should {

    "support tls" in withClient(cfg = withTls) { client =>
      client.toService(Request.Sync)
        .map { response =>
          response must beEqualTo(Response.Ready)
        }
    }
  }
}

class MissingTlsSpec extends PgSqlIntegrationSpec with ResourceFileSpec {

  def withTls(client: PostgreSql.Client): PostgreSql.Client =
    client.withTransport.tls(SslClientConfiguration(trustCredentials = TrustCredentials.Insecure))

  "TLS" should {
    "handle unsupported tls" in withClient(cfg = withTls) { client =>
      client.toService(Request.Sync)
        .liftToTry
        .map { response =>
          response.asScala must beAFailedTry(beEqualTo(PgSqlTlsUnsupportedError))
        }
    }
  }
}
