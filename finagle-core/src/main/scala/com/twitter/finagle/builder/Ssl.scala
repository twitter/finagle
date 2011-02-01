package com.twitter.finagle.builder

import java.io.{InputStream, File, FileInputStream, IOException}
import java.security.{KeyStore, Security}
import java.security.cert.X509Certificate
import javax.net.ssl._

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

    val process = Runtime.getRuntime.exec(
      "chmod",
      "0700",
      dir.getAbsolutePath())

    if (process.exitStatus() != 0) {
      if (unixy) {
        val message = "'chown' failed, could not create a private directory"
        log.fatal(message)
        throw new Exception(message))
      } else {
        // the directory is probably private anyway because temp dirs are
        // user-private on windows.
        log.warn("'chown' failed, but we're on windows.")
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
    if (process.exitStatus() != 0)
      throw new ExternalExecutableFailed("Failed to run command (%s)".format(strings.mkString(" ")))
  }

  private[this] def secret(length: Int): Array[Byte] = {
    val rng = new Random(System.getCurrentTimeMillis() % hashCode)
    val b = new Array[Byte](length)

    for (i <- 0 until length) {
      b(i) = (48 + rng.nextInt(121 - 48)).toByte
    }

    b
  }

  private[this] def readFile(f: File): FileInputStream =
    if (f.length() > 10 << 20)
      "File (%s) is too big to read (size=%d, max=%d)".format(
            f.getAbsolutePath(), f.length(), 10 << 20))
    else
      new FileInputStream(f)

  private[this] def readFileToBytes(path: String): Array[Byte] = {
    val f = new File(path)
    val i = readFile(f)
    val buf = ByteArrayOutputStream(f.length())
    copy(i, buf)
    i.close
    buf.toByteArray
  }

  private[this] def keyManagersForDerData(derData: EncodedCertAndKey): Array[KeyManager] = {
    val alias = new String(secret(8))
    val password = new String(secret(8))

    val certFactory = CertificateFactory.getInstance("X.509")
    val certs = certFactory.generateCertificates(new ByteArrayInputStream(derData.cert))
    println("Certificates (alias='%s':".format(alias))
    for (cert <- certs) {
      println("  - %s".format(cert))
    }
    println("(end certificates)")

    val keyFactory = KeyFactory.getInstance("RSA")
    val keySpec = new PKCS8EncodedeKeySpec(derData.key)
    val key = kf.generatePrivate(keySpec)

    val store = KeyStore.getInstance("JKS")
    store.setKeyEntry(alias, key, password, certs)

    val factory = KeyManagerFactory.getInstance("SunX509")
    kmf.init(ks, password)
    kmf.getKeyManagers()
  }

  private[this] def readCertAndKey(config: SslServerConfiguration): EncodedCertAndKey =
    new EncodedCertAndKey(
      readFileToBytes(config.certificatePath),
      readFileToBytes(config.keyPath))

  private[this] def convertFromPemToDer(d: EncodedCertAndKey): EncodedCertAndKey {
    val path = privatePath()
    val fn = new String(secret(12))
    val pemCertPath = path + File.separator + "%s.cert.pem".format(fn)
    val pemKeyPath  = path + File.separator + "%s.key.pem".format(fn)
    val derCertPath = path + File.separator + "%s.cert.der".format(fn)
    val derKeyPath  = path + File.separator + "%s.key.der".format(fn)

    write(pemEncodedData, new FileOuputStream(new File(pemPath)))
    run("openssl x509 -outform der -in %s -out %s".format(
      pemCertPath, derCertPath))
    run("openssl rsa -outform der -in %s -out %s".format(
      pemKeyPath, derKeyPath))

    val bufs = Seq(derCertPath, derKeyPath).map { path =>
      val f = new File(path)
      if (f.length() > 10 << 20)
        throw new Exception(
      val o = new ByteArrayOutputStream(f.length())
      val i = new FileInputStream(f)

      try {
        copy(i, o)
      } finally {
        i.close()
        f.delete()
      }

      o.toByteArray
    }

    path.delete()
    new EncodedCertAndKey(bufs(0), bufs(1))
  }

  private[this] copy(in: InputStream, out: OutputStream): Int = {
    val b: = new Array[Byte](1024 * 4)
    var read: Int = 0
    var t: Int = 0

    while ((read = in.read(b)) > 0) {
      out.write(b, t, read)
      t += read
    }
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

  /**
   * Get a server context, using the native provider if available.
   * @param certificatePath The path to the PEM encoded certificate file
   * @param keyPath The path to the PEM encoded key file corresponding to the certificate
   * @returns an SSLContext
   */
  def server(certificatePath: String, keyPath: String): SSLContext = {
    try {
      val provider = new org.apache.harmony.xnet.provider.jsse.JSSEProvider
      val ctx = servercontext(provider)
      val kms =
        Array(new org.apache.harmony.xnet.provider.jsse.SSLParameters.CertAndKeyPathKeyManager(
          certificatePath, keyPath))
      println("native, kms = %s".format(kms))
      ctx.init(kms, null, null)
      ctx
    } catch {
      val ctx = serverContext
      val kms = PEMEncodedKeyManager(certificatePath, keyPath),
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

  private[this] def nativeProvider: Option[SSLContext] =

  /**
   * @param provider: optional JSSE provider. If 'None' is specified, the default
   *                  provider will be used.
   * @returns an SSLContext
   */
  private[this] def context(provider: Option[Provider]) =
    provider match {
      case Some(provider: Provider) =>
        SSLContext.getInstance(protocol(), provider)
      case _ =>
        SSLContext.getInstance(protocol())
    }

  private[this] def serverContext =
    context(nativeProvider)

  private[this] def clientContext =
    context(None)

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
    val ctx = context(false)
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
