package com.twitter.finagle.mysql.integration

import com.twitter.finagle.mysql.HandshakeStackModifier
import com.twitter.finagle.mysql.harness.EmbeddedSuite
import com.twitter.finagle.mysql.harness.config.{DatabaseConfig, InstanceConfig}

class MysqlSslTest extends EmbeddedSuite {

  private val sslStartServerParameters: Seq[String] =
    defaultInstanceConfig.startServerParameters ++ SslTestUtils.getSslParameters

  val instanceConfig: InstanceConfig =
    defaultInstanceConfig.copy(startServerParameters = sslStartServerParameters)
  val databaseConfig: DatabaseConfig =
    DatabaseConfig(databaseName = "ssl_database", users = Seq.empty, setupQueries = Seq.empty)

  test("ping over ssl") { fixture =>
    val theClient = fixture
      .newClient().configured(
        HandshakeStackModifier(SslTestUtils.downgradedHandshake)).withTransport.tls.newRichClient(
        fixture.instance.dest)
    val result = await(theClient.ping())
  // If we get here, result is Unit, and all is good
  }

}
