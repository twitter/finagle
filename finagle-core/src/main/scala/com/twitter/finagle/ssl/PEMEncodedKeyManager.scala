package com.twitter.finagle.ssl

import java.io._
import java.security.cert.X509Certificate
import java.security.spec._
import java.security.{KeyFactory, KeyStore, Security, Provider, PrivateKey}
import java.util.Random
import javax.net.ssl._

import com.twitter.io.{Files, TempDirectory, StreamIO}

/*
 * Take a PEM-encoded cert and key, and turn it into a Java keystore
 * which is then loaded into a resulting KeyManager
 *
 * @param certificatePath the path to the PEM-encoded certificate
 * @param keyPath the path to the PEM-encoded private key
 * @returns Array[KeyManager]
 */
object PEMEncodedKeyManager {
  class ExternalExecutableFailed(message: String) extends Exception(message)

  def apply(certificatePath: String, keyPath: String): Array[KeyManager] =
    makeKeystore(
      Files.readBytes(new File(certificatePath)),
      Files.readBytes(new File(keyPath))
    )

  private[this] def secret(length: Int): Array[Char] = {
    val rng = new Random()
    rng.setSeed(System.currentTimeMillis())
    val b = new Array[Char](length)

    for (i <- 0 until length)
      b(i) = (65 + rng.nextInt(90 - 65)).toChar

    b
  }

  private[this] def makeKeystore(certificate: Array[Byte], key: Array[Byte]): Array[KeyManager] = {
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
    val jksPath = path + File.separator + "%s.jks".format(fn)

    // Write out the certificate and key
    val f = new FileOutputStream(new File(pemPath))
    StreamIO.copy(new ByteArrayInputStream(certificate), f)
    StreamIO.copy(new ByteArrayInputStream(key), f)
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

    // Convert the PKCS12 file into a Java keystore
    Shell.run(
      Array(
        "keytool",
        "-makeKeystore",
        "-srckeystore",  p12Path,
        "-srcstoretype", "PKCS12",
        "-destkeystore", jksPath,
        "-trustcacerts",
        "-srcstorepass", passwordStr,
        "-keypass",      passwordStr,
        "-storepass",    passwordStr
      )
    )

    // Read the resulting keystore
    val input = new ByteArrayInputStream(Files.readBytes(new File(jksPath)))
    val ks = KeyStore.getInstance("JKS")
    val kmf = KeyManagerFactory.getInstance("SunX509")
    ks.load(input, password)
    kmf.init(ks, password)

    // Clean up by deleting the files and directory
    Array(pemPath, p12Path, jksPath).foreach(new File(_).delete())
    path.delete()

    kmf.getKeyManagers
  }
}
