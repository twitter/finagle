package com.twitter.finagle.builder

import java.io.{InputStream, File, FileInputStream, IOException}
import java.security.{KeyStore, Security}
import java.security.cert.X509Certificate
import javax.net.ssl._

object Ssl {
  val defaultProtocol = "TLS"

  object Keys {
    val defaultAlgorithm = "SunX509"

    def managers(in: InputStream, password: String) = {
      KeyStore.getInstance("JKS") match {
        case ks: KeyStore =>
          val kmf = KeyManagerFactory.getInstance(defaultAlgorithm)
          val pw = password.toArray
          ks.load(in, pw)
          kmf.init(ks, pw)
          kmf.getKeyManagers
      }
    }
  }

  object Config {
    type StringPredicate = (String) => (Boolean)

    private[this] def filterCipherSuites(ctx: SSLContext,
                                         filters: Seq[StringPredicate]) {
      val params = ctx.getDefaultSSLParameters
      for (filter <- filters)
        params.setCipherSuites(params.getCipherSuites().filter(filter))
    }

    private[this] def disableAnonCipherSuites(ctx: SSLContext) {
      val excludeDiffieHellmanAnon = ((s:String) => s.indexOf("DH_anon") != -1)
      filterCipherSuites(ctx, Seq(excludeDiffieHellmanAnon))
    }

    private[this] def dispreferClientAuth(ctx: SSLContext) {
      ctx.getDefaultSSLParameters().setWantClientAuth(false)
      ctx.getDefaultSSLParameters().setNeedClientAuth(false)
    }

    def apply(ctx: SSLContext) {
      dispreferClientAuth(ctx)
      disableAnonCipherSuites(ctx)
    }
  }

  def server(path: String, password: String): SSLContext = {
    new File(path) match {
      case f: File
      if f.exists && f.canRead =>
        val kms = Keys.managers(new FileInputStream(f), password)
        val ctx = context()
        ctx.init(kms, null, null) // XXX: specify RNG?
        Config(ctx)
        ctx
      case _ =>
        throw new IOException(
          "Keystore file '%s' does not exist or is not readable".format(path))
    }
  }

  /**
   * @returns the protocol used to create SSLContext instances
   */
  def protocol() = defaultProtocol

  /**
   * @returns instances of SSLContext, supporting protocol()
   */
  def context() = SSLContext.getInstance(protocol())

  /**
   * Create a client
   */
  def client(): SSLContext = {
    val ctx = context()
    ctx.init(null, null, null)
    Config(ctx)
    ctx
  }

  /**
   * Create a client with a trust manager that does not check the validity of certificates
   */
  def clientWithoutCertificateValidation(): SSLContext = {
    val ctx = context()
    ctx.init(null, trustAllCertificates(), null)
    Config(ctx)
    ctx
  }

  /**
   * A trust manager that does not validate anything
   */
  private[this] class IgnorantTrustManager extends X509TrustManager {
    def getAcceptedIssuers(): Array[X509Certificate] =
      null

    def checkClientTrusted(certs: Array[X509Certificate],
                           authType: String) {
      // Do nothing.
    }

    def checkServerTrusted(certs: Array[X509Certificate],
                           authType: String) {
      // Do nothing.
    }
  }

  /**
   * @returns a trust manager chain that does not validate certificates
   */
  private[this] def trustAllCertificates(): Array[TrustManager] =
    Array(new IgnorantTrustManager)
}
