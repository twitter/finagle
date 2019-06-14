package com.twitter.finagle.mysql.integration

import com.twitter.finagle.Mysql
import com.twitter.finagle.ssl.{Protocols, TrustCredentials}
import com.twitter.finagle.ssl.client.SslClientConfiguration
import com.twitter.util.{Await, Duration}
import org.scalatest.FunSuite

class MysqlSslTest extends FunSuite with IntegrationClient {

  override protected def configureClient(
    username: String,
    password: String,
    db: String
  ): Mysql.Client = {
    // We care more about testing the SSL/TLS wiring throughout
    // MySQL here than verifying certificates and hostnames.
    //
    // The community edition of MySQL 5.7 isn't built by default
    // with OpenSSL, and so uses yaSSL, which only supports up to
    // TLSv1.1. So we use TLSv1.1 here.
    val sslClientConfig = SslClientConfiguration(
      trustCredentials = TrustCredentials.Insecure,
      protocols = Protocols.Enabled(Seq("TLSv1.1")))
    super
      .configureClient(username, password, db)
      .withTransport.tls(sslClientConfig)
  }

  test("ping over ssl") {
    val theClient = client.orNull
    val result = Await.result(theClient.ping(), Duration.fromSeconds(2))
    // If we get here, result is Unit, and all is good
  }

}
