package com.twitter.finagle.ssl

import java.util.logging.Logger
import java.security.cert.X509Certificate
import javax.net.ssl._

import collection.mutable.{Map => MutableMap}

/*
 * Creates JSSE SSLEngines on behalf of the Ssl singleton
 */
object JSSE {
  private[this] val log = Logger.getLogger(getClass.getName)
  private[this] val contextCache: MutableMap[String, SSLContext] = MutableMap.empty
  private[this] val protocol = "TLS"

  /**
   * Get a server
   */
  def server(certificatePath: String, keyPath: String, useCache: Boolean = true): Option[Engine] = {
    def makeContext: SSLContext = {
      val context = SSLContext.getInstance(protocol)
      val kms = PEMEncodedKeyManager(certificatePath, keyPath)
      context.init(kms, null, null)

      log.finest("JSSE context instantiated for certificate '%s'".format(
        certificatePath
      ))

      context
    }

    val context = synchronized {
      if (useCache)
        contextCache.getOrElseUpdate(certificatePath, makeContext)
      else
        makeContext
    }

    Some(new Engine(context.createSSLEngine()))
  }

  /**
   * Get a client
   */
  def client(): Engine = client(null)

  /**
   * Get a client that skips verification of certificates.
   *
   * Security Warning: This defeats the purpose of SSL.
   */
  def clientWithoutCertificateValidation(): Engine =
    client(trustAllCertificates())

  private[this] def client(trustManagers: Array[TrustManager]): Engine = {
    val ctx = SSLContext.getInstance(protocol)
    ctx.init(null, trustManagers, null)
    new Engine(ctx.createSSLEngine())
  }

  /**
   * @returns a trust manager chain that does not validate certificates
   */
  private[this] def trustAllCertificates(): Array[TrustManager] =
    Array(new IgnorantTrustManager)

  /**
   * A trust manager that does not validate anything
   */
  private[this] class IgnorantTrustManager extends X509TrustManager {
    def getAcceptedIssuers(): Array[X509Certificate] = new Array[X509Certificate](0)

    def checkClientTrusted(certs: Array[X509Certificate], authType: String) {
      // Do nothing.
    }

    def checkServerTrusted(certs: Array[X509Certificate], authType: String) {
      // Do nothing.
    }
  }
}
