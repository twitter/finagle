package com.twitter.finagle.http

class ClassicH2CEndToEndTest extends AbstractH2CEndToEndTest {
  def implName: String = "h2c http/2"
  def useMultiplexCodec: Boolean = false
}

class MultiplexedH2CEndToEndTest extends AbstractH2CEndToEndTest {
  def implName: String = "h2c http/2-multiplex"
  def useMultiplexCodec: Boolean = true

  // Should be fixed when https://github.com/netty/netty/issues/8434 is fixed.
  override def featureImplemented(feature: Feature): Boolean =
    feature != MaxHeaderSize && super.featureImplemented(feature)
}
