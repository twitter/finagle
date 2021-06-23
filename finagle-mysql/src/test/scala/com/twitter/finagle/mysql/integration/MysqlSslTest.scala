package com.twitter.finagle.mysql.integration

import com.twitter.finagle.Stack
import com.twitter.finagle.mysql.harness.EmbeddedSuite
import com.twitter.finagle.mysql.harness.config.{DatabaseConfig, InstanceConfig}
import com.twitter.finagle.mysql.{HandshakeInit, HandshakeStackModifier}
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.ssl.{Protocols, TrustCredentials}
import com.twitter.finagle.transport.Transport

class MysqlSslTest extends EmbeddedSuite {

  val baseSslCaPath: String = "/ssl/certs/mysql-server-ca.crt"
  val baseSslCertPath: String = "/ssl/certs/mysql-server.crt"
  val baseSslKeyPath: String = "/ssl/keys/mysql-server.key"

  private val sslParameters: Seq[String] = Seq(
    s"--ssl-ca=${getClass.getResource(baseSslCaPath).getPath}",
    s"--ssl-cert=${getClass.getResource(baseSslCertPath).getPath}",
    s"--ssl-key=${getClass.getResource(baseSslKeyPath).getPath}"
  )

  private val sslStartServerParameters: Seq[String] =
    defaultInstanceConfig.startServerParameters ++ sslParameters

  val instanceConfig: InstanceConfig =
    defaultInstanceConfig.copy(startServerParameters = sslStartServerParameters)
  val databaseConfig: DatabaseConfig =
    DatabaseConfig(databaseName = "ssl_database", users = Seq.empty, setupQueries = Seq.empty)

  private def downgradedHandshake(
    params: Stack.Params,
    handshakeInit: HandshakeInit
  ): Stack.Params = {
    // We care more about testing the SSL/TLS wiring throughout
    // MySQL here than verifying certificates and hostnames.
    //
    // The community edition of MySQL 5.7 isn't built by default
    // with OpenSSL, and so uses yaSSL, which only supports up to
    // TLSv1.1. So we use TLSv1.1 here.
    val mysql5Config = SslClientConfiguration(
      trustCredentials = TrustCredentials.Insecure,
      protocols = Protocols.Enabled(Seq("TLSv1.1")))
    val mysql8Config = mysql5Config.copy(protocols = Protocols.Enabled(Seq("TLSv1.2")))
    val selectedConfig = if (handshakeInit.version.startsWith("5.")) mysql5Config else mysql8Config
    params + Transport.ClientSsl(Some(selectedConfig))
  }

  test("ping over ssl") { fixture =>
    val theClient = fixture
      .newClient().configured(
        HandshakeStackModifier(downgradedHandshake)).withTransport.tls.newRichClient(
        fixture.instance.dest)
    val result = await(theClient.ping())
  // If we get here, result is Unit, and all is good
  }

}
