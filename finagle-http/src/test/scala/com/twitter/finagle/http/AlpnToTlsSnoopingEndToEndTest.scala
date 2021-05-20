package com.twitter.finagle.http

import com.twitter.finagle.Http
import com.twitter.finagle.http.ssl.HttpSslTestComponents
import com.twitter.finagle.param.OppTls
import com.twitter.finagle.ssl.OpportunisticTls
import com.twitter.finagle.ssl.server.SslServerConfiguration

class AlpnToTlsSnoopingEndToEndTest extends Http2AlpnTest {
  override def implName: String = "alpn http/2-multiplex with sniffing"

  override def serverConfiguration(): SslServerConfiguration =
    HttpSslTestComponents.unauthenticatedServerConfig

  override def serverImpl(): Http.Server =
    super
      .serverImpl()
      .configured(OppTls(Some(OpportunisticTls.Desired)))
}
