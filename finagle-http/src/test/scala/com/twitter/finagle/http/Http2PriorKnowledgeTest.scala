package com.twitter.finagle.http

class ClassicHttp2PriorKnowledgeTest extends AbstractHttp2PriorKnowledgeTest {
  def implName: String = "prior knowledge http/2"
  def useMultiplexCodec: Boolean = false
}

class Http2PriorKnowledgeTest extends AbstractHttp2PriorKnowledgeTest {
  def implName: String = "prior knowledge http/2"
  def useMultiplexCodec: Boolean = true

  // Should be fixed when https://github.com/netty/netty/issues/8434 is fixed.
  override def featureImplemented(feature: Feature): Boolean =
    feature != MaxHeaderSize && super.featureImplemented(feature)
}
