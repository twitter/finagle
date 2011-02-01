package com.twitter.finagle.builder

import java.util.logging.Logger
import java.io._
import java.security.{KeyFactory, KeyStore, Security, Provider, PrivateKey}
import java.security.cert.X509Certificate
import java.security.cert.{Certificate, CertificateFactory}
import java.security.spec._
import javax.net.ssl._
import java.util.Random

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

/**
 * Store the files necessary to configure an SSL
 */
case class SslServerConfiguration(
  val certificatePath: String,
  val keyPath: String)

object TemporaryDirectory {
  private[this] val log = Logger.getLogger(getClass.getName)
  private[this] val unixy: Boolean =
    System.getProperty("os.name").toLowerCase.indexOf("win") == -1

  def apply(): File = {
    var tmp = File.createTempFile("temp", "dir")
    tmp.delete()
    tmp.mkdir()
    tmp.deleteOnExit()
    tmp
  }

  def readableOnlyByMe(): File = {
    val dir = apply()

    val process = Runtime.getRuntime.exec(Array(
      "chmod",
      "0700",
      dir.getAbsolutePath()))
    process.waitFor()

    if (process.exitValue() != 0) {
      if (unixy) {
        val message = "'chown' failed, could not create a private directory"
        log.severe(message)
        throw new Exception(message)
      } else {
        // the directory is probably private anyway because temp dirs are
        // user-private on windows.
        log.warning("'chown' failed, but we're on windows.")
      }
    }

    dir
  }
}

/**
 * Creates KeyManagers for PEM files.
 */
object PEMEncodedKeyManager {
  private[this] case class EncodedCertAndKey(cert: Array[Byte], key: Array[Byte])

  private[this] def privatePath(): File =
    TemporaryDirectory.readableOnlyByMe()

  class ExternalExecutableFailed(message: String) extends Exception(message)

  private[this] def run(strings: Array[String]) {
    val process = Runtime.getRuntime.exec(strings)
    process.waitFor()
    if (process.exitValue() != 0)
      throw new ExternalExecutableFailed("Failed to run command (%s)".format(strings.mkString(" ")))
  }

  private[this] def secret(length: Int): Array[Byte] = {
    val rng = new Random(System.currentTimeMillis() % hashCode)
    val b = new Array[Byte](length)

    for (i <- 0 until length) {
      b(i) = (65 + rng.nextInt(90 - 65)).toByte
    }

    b
  }

  private[this] def readFile(f: File): FileInputStream =
    if (f.length() > (10 << 20))
      throw new Exception(
        "File (%s) is too big to read (size=%d, max=%d)".format(
          f.getAbsolutePath(), f.length(), 10 << 20))
    else
      new FileInputStream(f)

  private[this] def readFileToBytes(path: String): Array[Byte] = {
    val f = new File(path)
    val i = readFile(f)
    val buf = new ByteArrayOutputStream(f.length().intValue())
    copy(i, buf)
    i.close
    buf.toByteArray
  }

  private[this] def readCertAndKey(config: SslServerConfiguration): EncodedCertAndKey =
    new EncodedCertAndKey(
      readFileToBytes(config.certificatePath),
      readFileToBytes(config.keyPath))

  private[this] def p12Keystore(d: EncodedCertAndKey): Array[KeyManager] = {
    val path = privatePath()
    val password = secret(24)
    val passwordStr = new String(password)
    val fn = new String(secret(12))
    val pemPath = path + File.separator + "%s.pem".format(fn)
    val p12Path = path + File.separator + "%s.p12".format(fn)
    val jksPath = path + File.separator + "%s.jks".format(fn)

    val f = new FileOutputStream(new File(pemPath))
    copy(new ByteArrayInputStream(d.cert), f)
    copy(new ByteArrayInputStream(d.key), f)
    f.close()

    run(Array(
      "openssl", "pkcs12",
      "-export",
      "-password", "pass:%s".format(passwordStr),
      "-in", pemPath,
      "-out", p12Path))

    run(Array(
      "keytool",
      "-importkeystore",
      "-srckeystore", p12Path,
      "-srcstoretype", "PKCS12",
      "-destkeystore", jksPath,
      "-trustcacerts",
      "-srcstorepass", passwordStr,
      "-keypass", passwordStr,
      "-storepass", passwordStr))

    val passwordChars = passwordStr.toCharArray
    val ksInput = new ByteArrayInputStream(readFileToBytes(jksPath))

    // new File(pemPath).delete()
    // new File(p12Path).delete()
    // new File(jksPath).delete()
    Seq(pemPath, p12Path, jksPath).foreach(new File(_).delete())
    path.delete()

    val ks = KeyStore.getInstance("JKS")
    val kmf = KeyManagerFactory.getInstance("SunX509")
    ks.load(ksInput, passwordChars)
    kmf.init(ks, passwordChars)
    kmf.getKeyManagers
  }

  private[this] def copy(in: InputStream, out: OutputStream): Int = {
    val b = new Array[Byte](1024 * 4)
    var read = 0
    var t = 0

    do {
      read = in.read(b)
      if (read > 0)
        out.write(b, t, read)
      t += read
    } while (read > 0)

    out.flush()

    t
  }

  def apply(config: SslServerConfiguration): Array[KeyManager] =
    p12Keystore(readCertAndKey(config))
}

object Ssl {
  val defaultProtocol = "TLS"

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

    def apply(ctx: SSLContext) {
      disableAnonCipherSuites(ctx)
    }
  }

  object Harmony {
    def provider(): Provider = {
      val name = "org.apache.harmony.xnet.provider.jsse.JSSEProvider"
      Class.forName(name).newInstance().asInstanceOf[Provider]
    }

    def keyManager(certificatePath: String, keyPath: String): KeyManager = {
      val name = "org.apache.harmony.xnet.provider.jsse.JSSEProvider.CertAndKeyPathKeyManager"
      val constructor = Class.forName(name).getConstructor(classOf[String], classOf[String])
      constructor.newInstance(certificatePath, keyPath).asInstanceOf[KeyManager]
    }
  }

  /**
   * Get a server context, using the native provider if available.
   * @param certificatePath The path to the PEM encoded certificate file
   * @param keyPath The path to the PEM encoded key file corresponding to the certificate
   * @returns an SSLContext
   */
  def server(certificatePath: String, keyPath: String): SSLContext = {
    try {
      val provider = Harmony.provider()
      val ctx = SSLContext.getInstance(protocol(), provider)
      val kms =
        Array(Harmony.keyManager(certificatePath, keyPath))
      println("native, kms = %s".format(kms))
      ctx.init(kms, null, null)
      ctx
    } catch {
      case _ =>
        val ctx = SSLContext.getInstance(protocol())
        val kms = PEMEncodedKeyManager(new SslServerConfiguration(certificatePath, keyPath))
        ctx.init(kms, null, null)
        Config(ctx)
        ctx
    }
  }

  /**
   * @returns the protocol used to create SSLContext instances
   */
  def protocol() = defaultProtocol


  /**
   * @return an SSLContext provisioned to be a client
   */
  private[this] def clientContext =
    SSLContext.getInstance(protocol())

  /**
   * Create a client
   */
  def client(): SSLContext = {
    val ctx = clientContext
    ctx.init(null, null, null)
    Config(ctx)
    ctx
  }

  /**
   * Create a client with a trust manager that does not check the validity of certificates
   */
  def clientWithoutCertificateValidation(): SSLContext = {
    val ctx = clientContext
    ctx.init(null, trustAllCertificates(), null)
    Config(ctx)
    ctx
  }

  /**
   * A trust manager that does not validate anything
   */
  private[this] class IgnorantTrustManager extends X509TrustManager {
    def getAcceptedIssuers(): Array[X509Certificate] =
      new Array[X509Certificate](0)

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
