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

  private[this] def context(protocol: String, kms: Array[KeyManager]) = {
    val ctx = SSLContext.getInstance(protocol)
    ctx.init(kms, null, null) // XXX: specify RNG?
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
