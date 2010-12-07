package com.twitter.finagle.builder

import java.io.{InputStream, File, FileInputStream, IOException}
import java.security.{KeyStore, Security}
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

  private[this] def context(protocol: String, kms: Array[KeyManager]) = {
    val ctx = SSLContext.getInstance(protocol)
    ctx.init(kms, null, null) // XXX: specify RNG?
    Config(ctx)
    ctx
  }

  def apply(path: String, password: String): SSLContext = {
    new File(path) match {
      case f: File
      if f.exists && f.canRead =>
        context(defaultProtocol, Keys.managers(new FileInputStream(f), password))
      case _ =>
        throw new IOException(
          "Keystore file '%s' does not exist or is not readable".format(path))
    }
  }
}
