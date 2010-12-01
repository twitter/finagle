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

// public class SecureChatSslContextFactory {
//       private static final String PROTOCOL = "TLS";
//       private static final SSLContext SERVER_CONTEXT;
//       private static final SSLContext CLIENT_CONTEXT;
//       static {
//           String algorithm = Security.getProperty("ssl.KeyManagerFactory.algorithm");
//           if (algorithm == null) {
//               algorithm = "SunX509";
//           }
//           SSLContext serverContext = null;
//           SSLContext clientContext = null;
//           try {
//               KeyStore ks = KeyStore.getInstance("JKS");
//               ks.load(SecureChatKeyStore.asInputStream(),
//                       SecureChatKeyStore.getKeyStorePassword());
//               // Set up key manager factory to use our key store
//               KeyManagerFactory kmf = KeyManagerFactory.getInstance(algorithm);
//               kmf.init(ks, SecureChatKeyStore.getCertificatePassword());
//               // Initialize the SSLContext to work with our key managers.
//               serverContext = SSLContext.getInstance(PROTOCOL);
//               serverContext.init(kmf.getKeyManagers(), null, null);
//           } catch (Exception e) {
//               throw new Error(
//                       "Failed to initialize the server-side SSLContext", e);
//           }
//           try {
//               clientContext = SSLContext.getInstance(PROTOCOL);
//               clientContext.init(null, SecureChatTrustManagerFactory.getTrustManagers(), null);
//           } catch (Exception e) {
//               throw new Error(
//                       "Failed to initialize the client-side SSLContext", e);
//           }
//           SERVER_CONTEXT = serverContext;
//           CLIENT_CONTEXT = clientContext;
//       }
//      public static SSLContext getServerContext() {
//          return SERVER_CONTEXT;
//      }
//      public static SSLContext getClientContext() {
//          return CLIENT_CONTEXT;
//      }
//  }
