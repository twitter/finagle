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
   * Get a server engine, using the native OpenSSL provider if available.
   *
   * @param certificatePath The path to the PEM encoded certificate file
   * @param keyPath The path to the corresponding PEM encoded key file
   * @param caCertPath [OpenSSL] The path to the optional PEM encoded CA cert file
   * @param cipherSpec [OpenSSL] The cipher spec
   * @throws RuntimeException if no provider could be initialized
   * @returns an SSLEngine
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

    nativeInstance.getOrElse {
      require(caCertPath == null, "'CA Certificate' parameter unsupported with JSSE SSL provider")
      require(ciphers == null, "'Ciphers' parameter unsupported with JSSE SSL provider")
      require(nextProtos == null, "'Next Protocols' parameter unsupported with JSSE SSL provider")

      val jsseInstance = JSSE.server(
        certificatePath,
        keyPath,
        cacheContexts
      )

      require(jsseInstance.isDefined, "Could not create an SSLEngine")

      jsseInstance.get
    }
  }

  /**
   * Get a client engine
   */
  def client(): Engine = JSSE.client()

  /**
   * Get a client engine that doesn't check the validity of certificates
   *
   * N.B.: This is probably a bad idea for anything but testing!
   */
  def clientWithoutCertificateValidation(): Engine =
    JSSE.clientWithoutCertificateValidation()
}
