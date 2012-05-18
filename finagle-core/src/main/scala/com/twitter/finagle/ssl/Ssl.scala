package com.twitter.finagle.ssl

import java.security.cert.{Certificate, CertificateFactory}
import java.util.logging.{Level, Logger}
import javax.net.ssl._

import org.jboss.netty.channel.{
  Channel, ChannelHandlerContext, ChannelLocal, MessageEvent, SimpleChannelHandler
}

import collection.JavaConversions._
import collection.mutable.{Map => MutableMap}

/*
 * SSL helper object, capable of creating cached SSLEngine instances
 * backed by both the native APR/OpenSSL bindings, or pure Java JSSE.
 */
object Ssl {
  private[this] val log = Logger.getLogger(getClass.getName)
  private[this] val cacheContexts = true

  /**
   * Get a server engine, using the native OpenSSL provider.
   *
   * @param certificatePath The path to the PEM encoded certificate file
   * @param keyPath The path to the corresponding PEM encoded key file
   * @param caCertPath The path to the optional PEM encoded CA cert file
   * @param ciphers The cipherspec (http://www.openssl.org/docs/apps/ciphers.html)
   * @param nextProtos Next protocol.
   * @throws RuntimeException if no provider could be initialized
   * @return an SSLEngine
   */
  def server(certificatePath: String,
             keyPath: String,
             caCertPath: String,
             ciphers: String,
             nextProtos: String): Engine = {
    val nativeInstance = OpenSSL.server(
      certificatePath,
      keyPath,
      caCertPath,
      ciphers,
      nextProtos,
      cacheContexts
    )

    require(nativeInstance.isDefined, "Could not create an SSLEngine using native OpenSSL provider.")

    nativeInstance.get
  }

  /**
   * Get a server engine, using the JSSE implementation.
   *
   * @param certificatePath The path to the PEM encoded certificate file
   * @param keyPath The path to the corresponding PEM encoded key file
   * @param ciphers http://docs.oracle.com/javase/1.5.0/docs/guide/security/jsse/JSSERefGuide.html#PLUG
   * @throws RuntimeException if no provider could be initialized
   * @return an SSLEngine
   */
  def server(certificatePath: String,
             keyPath: String,
             ciphers: Array[String]): Engine = {
    val jsseInstance = JSSE.server(
      certificatePath,
      keyPath,
      ciphers,
      cacheContexts
    )

    require(jsseInstance.isDefined, "Could not create an SSLEngine")

    jsseInstance.get
  }

  /**
   * Get a dummy client engine
   */
  def client(): Engine = JSSE.client()

  /**
   * Get a client engine, from the given context
   */
  def client(sslContext : SSLContext): Engine = JSSE.client(sslContext)

  /**
   * Get a client engine that doesn't check the validity of certificates
   *
   * N.B.: This is probably a bad idea for anything but testing!
   */
  def clientWithoutCertificateValidation(): Engine =
    JSSE.clientWithoutCertificateValidation()
}
