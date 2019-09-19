package com.twitter.finagle.http

class Http2AlpnTest extends AbstractHttp2AlpnTest {
  def implName: String = "alpn http/2-multiplex"

  // MaxHeaderSize should be allowed when https://github.com/netty/netty/issues/8434 is fixed.
  // The RequiresAsciiFilter is due to Netty filtering non-ASCII characters in the h2 pipeline.
  override def featureImplemented(feature: Feature): Boolean =
    feature != MaxHeaderSize && feature != RequiresAsciiFilter && super.featureImplemented(feature)
}
