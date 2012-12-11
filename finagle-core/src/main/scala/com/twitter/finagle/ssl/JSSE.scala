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
  private[this] lazy val defaultSSLContext: SSLContext = {
    val ctx = SSLContext.getInstance(protocol)
    ctx.init(null, null, null)
    ctx
  }

  /**
   * Get an SSL server via JSSE
   *
   * @param certificatePath The path to the PEM encoded certificate file
   * @param keyPath The path to the corresponding PEM encoded key file
   * @param caCertPath The path to the optional PEM encoded CA cert file.
   *   If caCertPath is set, use it in setting up the connection instead of
   *   certificatePath. The cert chain should contain the certificate.
   * @param useCache Use a cache of SSL contexts, keyed on certificatePath
   * @throws RuntimeException if no provider could be initialized
   * @return an SSLEngine
   */
  private[finagle] def server(
    certificatePath: String,
    keyPath: String,
    caCertPath: Option[String],
    useCache: Boolean = true
  ): Option[Engine] = {
    def makeContext: SSLContext = {
      val context = SSLContext.getInstance(protocol)
      val kms = PEMEncodedKeyManager(
        certificatePath,
        keyPath,
        caCertPath)
      context.init(kms, null, null)

      log.finest("JSSE context instantiated for certificate '%s'".format(
        certificatePath
      ))

      context
    }

    val context = synchronized {
      if (useCache)
        contextCache.getOrElseUpdate(
          List(certificatePath, keyPath, caCertPath).mkString(" + "),
          makeContext
        )
      else
        makeContext
    }

    Some(new Engine(context.createSSLEngine()))
  }

  /**
   * Get a client
   */
  def client(): Engine = new Engine(defaultSSLContext.createSSLEngine())

  /**
   * Get a client from the given Context
   */
  def client(ctx : SSLContext) : Engine = {
    val sslEngine = ctx.createSSLEngine();
    sslEngine.setUseClientMode(true);
    new Engine(sslEngine)
  }

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
   * @return a trust manager chain that does not validate certificates
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
