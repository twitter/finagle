package com.twitter.finagle.ssl

import java.util.logging.Logger
import javax.net.ssl._


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
   * @param certificatePath The path to the PEM encoded certificate file.
   * @param keyPath The path to the corresponding PEM encoded key file
   * @param caCertPath The path to the optional PEM encoded CA cert
   *   file. [JSSE: If caCertPath is set, it should contain the
   *   certificate and will be used in place of certificatePath.]
   * @param cipherSpec [OpenSSL] The cipher spec
   * @throws RuntimeException if no provider could be initialized
   * @return an SSLEngine
   */
  def server(
    certificatePath: String,
    keyPath: String,
    caCertPath: String,
    ciphers: String,
    nextProtos: String
  ): Engine = {
    val nativeInstance = OpenSSL.server(
      certificatePath,
      keyPath,
      caCertPath,
      ciphers,
      nextProtos,
      cacheContexts
    )

    nativeInstance.getOrElse {
      require(ciphers == null, "'Ciphers' parameter unsupported with JSSE SSL provider")
      require(nextProtos == null, "'Next Protocols' parameter unsupported with JSSE SSL provider")

      val jsseInstance = JSSE.server(
        certificatePath,
        keyPath,
        if (caCertPath == null) None else Some(caCertPath),
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
   * Get a client engine from the given context
   */
  def client(sslContext : SSLContext): Engine = JSSE.client(sslContext)

  /**
   * Get a client engine that doesn't check the validity of certificates
   *
   * N.B.: This is probably a bad idea for anything but testing!
   */
  def clientWithoutCertificateValidation(): Engine =
    JSSE.clientWithoutCertificateValidation()

  /**
   * Get a client engine
   */
  def client(peerHost: String, peerPort: Int): Engine = JSSE.client(peerHost, peerPort)

  /**
   * Get a client engine from the given context
   */
  def client(sslContext : SSLContext, peerHost: String, peerPort: Int): Engine = JSSE.client(sslContext, peerHost, peerPort)

  /**
   * Get a client engine that doesn't check the validity of certificates
   *
   * N.B.: This is probably a bad idea for anything but testing!
   */
  def clientWithoutCertificateValidation(peerHost: String, peerPort: Int): Engine =
    JSSE.clientWithoutCertificateValidation(peerHost, peerPort)
}
