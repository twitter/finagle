package com.twitter.finagle.ssl

import com.twitter.io.{TempDirectory, StreamIO}
import java.io._
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl._

/**
 * Take a PEM-encoded cert and key and turn it into a PKCS#12 keystore
 * via openssl, which is then loaded into a resulting KeyManager
 *
 * @param certificatePath the path to the PEM-encoded certificate
 * @param keyPath the path to the PEM-encoded private key
 * @param caCertPath the path to the PEM-encoded intermediate/root certs;
 * multiple certs should be concatenated into a single file. If caCertPath
 *   is set, use it in setting up the connection instead of certificatePath.
 *   The cert chain should contain the certificate.
 * @return Array[KeyManager]
 */
object PEMEncodedKeyManager {
  class ExternalExecutableFailed(message: String) extends Exception(message)

  def apply(
    certificatePath: String,
    keyPath: String,
    caCertPath: Option[String]
  ): Array[KeyManager] =
    asStream(keyPath) { keyStream =>
      // if the chain is present, use it instead of the cert (chain contains cert)
      asStream(caCertPath.getOrElse(certificatePath)) { certificateStream =>
         makeKeystore(certificateStream, keyStream)
      }
    }

  private[this] def secret(length: Int): Array[Char] = {
    val rng = new SecureRandom()
    val b = new Array[Char](length)

    for (i <- 0 until length)
      b(i) = (65 + rng.nextInt(90 - 65)).toChar

    b
  }

  private[this] def asStream[T](filename: String)(f: (InputStream) => T): T = {
    val stream = new FileInputStream(filename)
    try {
      f(stream)
    } finally {
      stream.close()
    }
  }

  private[this] def makeKeystore(
    certificateStream: InputStream,
    keyStream: InputStream
  ) : Array[KeyManager] = {

    // Create a secure directory for the conversion
    val path = TempDirectory.create()
    Shell.run(Array("chmod", "0700", path.getAbsolutePath()))

    // Guard the keystore with a randomly-generated password
    val password = secret(24)
    val passwordStr = new String(password)

    // Use non-deterministic file names
    val fn = new String(secret(12))
    val pemPath = path + File.separator + "%s.pem".format(fn)
    val p12Path = path + File.separator + "%s.p12".format(fn)

    // Write out the certificate and key
    val f = new FileOutputStream(new File(pemPath))
    StreamIO.copy(certificateStream, f)
    StreamIO.copy(keyStream, f)
    f.close()

    // Import the PEM-encoded certificate and key to a PKCS12 file
    Shell.run(
      Array(
        "openssl",   "pkcs12",
        "-export",
        "-password", "pass:%s".format(passwordStr),
        "-in",       pemPath,
        "-out",      p12Path
      )
    )

    // Read the resulting keystore
    val keystore = asStream(p12Path) { stream =>
      val ks = KeyStore.getInstance("pkcs12")
      ks.load(stream, password)
      ks
    }

    // Clean up by deleting the files and directory
    Seq(pemPath, p12Path).foreach(new File(_).delete())
    path.delete()

    val kmf = KeyManagerFactory.getInstance("SunX509")
    kmf.init(keystore, password)
    kmf.getKeyManagers
  }
}
