package com.twitter.finagle.http

import java.io.{InputStream, File, FileInputStream}
import java.security.{KeyStore, Security}
import javax.net.ssl._

object Ssl {
  private[this] val protocol = "TLS"

  private[this] object KeyStoreFactory {
    val keystoreFilename = System.getProperty("ssl.keystore.file", "keystore.jks")
    val keystoreFile: File = new File(keystoreFilename)
    def keystoreInput: InputStream = new FileInputStream(keystoreFile)
    val password = "password".toArray

    def apply() = {
      val store = KeyStore.getInstance("JKS")
      store.load(keystoreInput, password)
      store
    }
  }

  private[this] object KeyManagers {
    val defaultAlgorithm = "SunX509"
    val algorithm =
      Option(Security.getProperty("ssl.KeyManagerFactory.algorithm"))
        .getOrElse(defaultAlgorithm)

    def apply() = {
      val kmf = KeyManagerFactory.getInstance(algorithm)
      kmf.init(KeyStoreFactory(), KeyStoreFactory.password);

      kmf.getKeyManagers
    }
  }

  val serverContext = SSLContext.getInstance(protocol)
  serverContext.init(KeyManagers(), null, null)

  def newServerContext() = {
    serverContext
  }
}
