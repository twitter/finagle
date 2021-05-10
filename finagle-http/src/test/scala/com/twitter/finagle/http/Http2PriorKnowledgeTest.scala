package com.twitter.finagle.http

class Http2PriorKnowledgeTest extends AbstractHttp2PriorKnowledgeTest {
  def implName: String = "prior knowledge http/2"

  // MaxHeaderSize should be allowed when https://github.com/netty/netty/issues/8434 is fixed.
  override def featureImplemented(feature: Feature): Boolean =
    feature != MaxHeaderSize && super.featureImplemented(feature)
}
