package com.twitter.finagle.http

class Http2AlpnTest extends AbstractHttp2AlpnTest {
  def implName: String = "alpn http/2-multiplex"

  // MaxHeaderSize should be allowed when https://github.com/netty/netty/issues/8434 is fixed.
  override def featureImplemented(feature: Feature): Boolean =
    feature != MaxHeaderSize && super.featureImplemented(feature)
}
