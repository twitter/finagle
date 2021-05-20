package com.twitter.finagle.http

import com.twitter.finagle
import com.twitter.finagle.http.ssl.HttpSslTestComponents
import com.twitter.finagle.param.OppTls
import com.twitter.finagle.ssl.{OpportunisticTls, SnoopingLevelInterpreter}
import com.twitter.finagle.transport.Transport

// Test of a H2C cleartext client to the TLS snooping H2 compatible server.
class H2CToTlsSnoopingEndToEndTest extends H2CEndToEndTest {

  override def implName: String = "h2cToTls"

  override def serverImpl(): finagle.Http.Server =
    super
      .serverImpl()
      .configured(SnoopingLevelInterpreter.EnabledForNonNegotiatingProtocols)
      .configured(Transport.ServerSsl(Some(HttpSslTestComponents.unauthenticatedServerConfig)))
      .configured(OppTls(Some(OpportunisticTls.Desired)))
}
