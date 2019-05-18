package com.twitter.finagle.http

class ClassicHttp2PriorKnowledgeTest extends AbstractHttp2PriorKnowledgeTest {
  def implName: String = "prior knowledge http/2"
  def useMultiplexCodec: Boolean = false

  override def featureImplemented(feature: Feature): Boolean =
    feature != RequiresAsciiFilter && super.featureImplemented(feature)
}

class Http2PriorKnowledgeTest extends AbstractHttp2PriorKnowledgeTest {
  def implName: String = "prior knowledge http/2"
  def useMultiplexCodec: Boolean = true

  // MaxHeaderSize should be allowed when https://github.com/netty/netty/issues/8434 is fixed.
  // The RequiresAsciiFilter is due to Netty filtering non-ASCII characters in the h2 pipeline.
  override def featureImplemented(feature: Feature): Boolean =
    feature != MaxHeaderSize && feature != RequiresAsciiFilter && super.featureImplemented(feature)
}
