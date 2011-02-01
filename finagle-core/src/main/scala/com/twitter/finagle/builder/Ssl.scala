package com.twitter.finagle.builder

import java.util.logging.Logger
import java.io._
import java.security.{KeyFactory, KeyStore, Security, Provider}
import java.security.cert.X509Certificate
import java.security.cert.{Certificate, CertificateFactory}
import java.security.spec._
import javax.net.ssl._
import java.util.Random

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
    if (process.exitValue() != 0)
      throw new ExternalExecutableFailed("Failed to run command (%s)".format(strings.mkString(" ")))
  }

  private[this] def secret(length: Int): Array[Byte] = {
    val rng = new Random(System.currentTimeMillis() % hashCode)
    val b = new Array[Byte](length)

    for (i <- 0 until length) {
      b(i) = (48 + rng.nextInt(121 - 48)).toByte
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

  private[this] def keyManagersForDer(derData: EncodedCertAndKey): Array[KeyManager] = {
    val alias = new String(secret(8))
    val password = secret(8).map(_.toChar)
    val certFactory = CertificateFactory.getInstance("X.509")
    val certs =
      certFactory.generateCertificates(
        new ByteArrayInputStream(derData.cert)).asInstanceOf[Iterable[Certificate]]
    println("Certificates (alias='%s':".format(alias))
    for (cert <- certs) {
      println("  - %s".format(cert))
    }
    println("(end certificates)")

    val keyFactory = KeyFactory.getInstance("RSA")
    val keySpec = new PKCS8EncodedKeySpec(derData.key)
    val key = keyFactory.generatePrivate(keySpec)

    val store = KeyStore.getInstance("JKS")
    store.setKeyEntry(alias, key, password, certs.toArray)

    val factory = KeyManagerFactory.getInstance("SunX509")
    factory.init(store, password)
    factory.getKeyManagers()
  }

  private[this] def readCertAndKey(config: SslServerConfiguration): EncodedCertAndKey =
    new EncodedCertAndKey(
      readFileToBytes(config.certificatePath),
      readFileToBytes(config.keyPath))

  private[this] def convertFromPemToDer(d: EncodedCertAndKey): EncodedCertAndKey = {
    val path = privatePath()
    val fn = new String(secret(12))
    val pemCertPath = path + File.separator + "%s.cert.pem".format(fn)
    val pemKeyPath  = path + File.separator + "%s.key.pem".format(fn)
    val derCertPath = path + File.separator + "%s.cert.der".format(fn)
    val derKeyPath  = path + File.separator + "%s.key.der".format(fn)

    copy(new ByteArrayInputStream(d.cert), new FileOutputStream(new File(pemCertPath)))
    copy(new ByteArrayInputStream(d.key), new FileOutputStream(new File(pemKeyPath)))
    run(Array(
      "openssl", "x509", "-outform", "der", "-in", pemCertPath, "-out", derCertPath))
    run(Array(
      "openssl", "rsa", "-outform", "der", "-in", pemKeyPath, "-out", derKeyPath))

    val bufs = Seq(derCertPath, derKeyPath).map(readFileToBytes(_))
    new EncodedCertAndKey(bufs(0), bufs(1))
  }

  private[this] def copy(in: InputStream, out: OutputStream): Int = {
    val b = new Array[Byte](1024 * 4)
    var read = 0
    var t = 0

    do {
      read = in.read(b)
      out.write(b, t, read)
      t += read
    } while (read > 0)

    out.flush()

    t
  }

  def apply(config: SslServerConfiguration): Array[KeyManager] =
    keyManagersForDer(
      convertFromPemToDer(
        readCertAndKey(config)))
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
        println("java, jms = %s".format(kms))
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
