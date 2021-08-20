package com.twitter.finagle.netty4.ssl

import io.netty.buffer.ByteBufAllocator
import io.netty.handler.ssl.{ApplicationProtocolNegotiator, SslContext}
import io.netty.util.ReferenceCountUtil
import java.util.{List => JList}
import javax.net.ssl.{SSLEngine, SSLSessionContext}

// A proxy `SslContext` that includes a finalizer for releasing a ref-counted
// underlying `SslContext`. This allows us to use the Netty ref-counted TLS
// implementation, which reduces GC times by cleaning up individual `SSLEngine`s
// via ref-counting as opposed to finalizers, without needing to explicitly
// manage the `SslContext` that is shared by all connections.
private class FinalizedSslContext(underlying: SslContext) extends SslContext {
  def isClient: Boolean = underlying.isClient

  def cipherSuites(): JList[String] = underlying.cipherSuites()

  override def sessionCacheSize(): Long = underlying.sessionCacheSize()

  override def sessionTimeout(): Long = underlying.sessionTimeout()

  def applicationProtocolNegotiator(): ApplicationProtocolNegotiator =
    underlying.applicationProtocolNegotiator()

  def newEngine(alloc: ByteBufAllocator): SSLEngine = underlying.newEngine(alloc)

  def newEngine(alloc: ByteBufAllocator, peerHost: String, peerPort: Int): SSLEngine =
    underlying.newEngine(alloc, peerHost, peerPort)

  def sessionContext(): SSLSessionContext = underlying.sessionContext()

  override def finalize(): Unit = {
    super.finalize()
    ReferenceCountUtil.safeRelease(underlying)
  }
}
