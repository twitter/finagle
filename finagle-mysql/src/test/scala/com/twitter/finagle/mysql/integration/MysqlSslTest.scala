package com.twitter.finagle.mysql.integration

import com.twitter.finagle.mysql.{HandshakeInit, HandshakeStackModifier}
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.finagle.ssl.{Protocols, TrustCredentials}
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Mysql, Stack}
import com.twitter.util.{Await, Duration}
import org.scalatest.funsuite.AnyFunSuite

class MysqlSslTest extends AnyFunSuite with IntegrationClient {

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

  override protected def configureClient(
    username: String,
    password: String,
    db: String
  ): Mysql.Client = {
    super
      .configureClient(username, password, db).configured(
        HandshakeStackModifier(downgradedHandshake)).withTransport.tls
  }

  test("ping over ssl") {
    val theClient = client.orNull
    val result = Await.result(theClient.ping(), Duration.fromSeconds(2))
    // If we get here, result is Unit, and all is good
  }

}
