package com.twitter.finagle.builder

import java.util.logging.{Level, Logger}
import java.io._
import java.security.{KeyFactory, KeyStore, Security, Provider, PrivateKey}
import java.security.cert.X509Certificate
import java.security.cert.{Certificate, CertificateFactory}
import java.security.spec._
import javax.net.ssl._
import java.util.Random

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.util.control.Breaks._

import org.jboss.netty.channel.{Channel, ChannelHandlerContext, ChannelLocal,
                                MessageEvent, SimpleChannelHandler}

/**
 * Store the files necessary to configure an SSL
 */
case class SslServerConfiguration(
  val certificatePath: String,
  val keyPath: String)


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
      throw new ExternalExecutableFailed(
        "Failed to run command (%s)".format(strings.mkString(" ")))
  }

  private[this] def secret(length: Int): Array[Byte] = {
    val rng = new Random()
    val b = new Array[Byte](length)

    for (i <- 0 until length)
      b(i) = (65 + rng.nextInt(90 - 65)).toByte

    b
  }

  private[this] def readFile(f: File): FileInputStream =
    if (f.length() > (10 << 20))
      throw new IOException("File length exceeds buffer size")
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

  private[this] def importKeystore(d: EncodedCertAndKey): Array[KeyManager] = {
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
    importKeystore(readCertAndKey(config))
}

object Ssl {
  private[this] val log = Logger.getLogger(getClass.getName)
  private[this] val defaultProtocol = "TLS"

  def isNativeProviderAvailable(): Boolean =
    try {
      val name = "org.apache.harmony.xnet.provider.jsse.JSSEProvider"
      Class.forName(name)
      true
    } catch {
      case c: ClassNotFoundException =>
        false
    }

  object DefaultJSSEConfig {
    type StringPredicate = (String) => (Boolean)

    private[this] def filterCipherSuites(ctx: SSLContext,
                                         filters: Seq[StringPredicate]) {
      val params = ctx.getDefaultSSLParameters
      for (filter <- filters)
        params.setCipherSuites(params.getCipherSuites().filter(filter))
    }

    private[this] def disableAnonCipherSuites(ctx: SSLContext) {
      val excludeDHAnon = ((s: String) => s.indexOf("DH_anon") != -1)
      filterCipherSuites(ctx, Seq(excludeDHAnon))
    }

    def apply(ctx: SSLContext) {
      disableAnonCipherSuites(ctx)
    }
  }

  object NativeJSSEContextFactory extends ContextFactory {
    def name() = "Native (Apache Harmony OpenSSL) Provider"

    def context(certificatePath: String, keyPath: String) = {
      val ctx = SSLContext.getInstance(protocol(), provider())
      val kms = Array(keyManager(certificatePath, keyPath))
      ctx.init(kms, null, null)
      ctx
    }

    private[this] def provider(): Provider = {
      val name = "org.apache.harmony.xnet.provider.jsse.JSSEProvider"
      Class.forName(name).newInstance().asInstanceOf[Provider]
    }

    private[this] def keyManager(certificatePath: String, keyPath: String): KeyManager = {
      val name = "org.apache.harmony.xnet.provider.jsse.CertAndKeyPathKeyManager"
      val constructor = Class.forName(name).getConstructor(classOf[String], classOf[String])
      constructor.newInstance(certificatePath, keyPath).asInstanceOf[KeyManager]
    }
  }

  object DefaultJSSEContextFactory extends ContextFactory {
    def name() = "JSSE Default Provider"

    def context(certificatePath: String, keyPath: String) = {
      val ctx = SSLContext.getInstance(protocol())
      val kms = PEMEncodedKeyManager(new SslServerConfiguration(certificatePath, keyPath))
      ctx.init(kms, null, null)
      Ssl.DefaultJSSEConfig(ctx)
      ctx
    }
  }

  trait ContextFactory {
    def name(): String
    def context(certificatePath: String, keyPath: String): SSLContext
  }

  val contextFactories: Seq[ContextFactory] =
    Seq(NativeJSSEContextFactory, DefaultJSSEContextFactory)

  class NoSuitableSslProvider(message: String) extends Exception(message: String)

  private[this] def fileMustExist(path: String) =
    require(new File(path).exists(), "File '%s' does not exist.".format(path))

  /**
   * Get a server context, using the native provider if available.
   * @param certificatePath The path to the PEM encoded certificate file
   * @param keyPath The path to the PEM encoded key file corresponding to the certificate
   * @throws a NoSuitableSslProvider if no provider could be initialized
   * @returns an SSLContext
   */
  def server(certificatePath: String, keyPath: String): SSLContext = {
    fileMustExist(certificatePath)
    fileMustExist(keyPath)
    var context: SSLContext = null

    for (factory <- contextFactories)
    if (context == null) {
      try {
        context = factory.context(certificatePath, keyPath)
      } catch {
        case e: Throwable =>
          log.log(Level.FINE, "Provider '%s' not suitable".format(factory.name()))
          log.log(Level.FINEST, e.getMessage, e)
      }
    }

    if (context != null)
      return context
    else
      throw new NoSuitableSslProvider(
        "No SSL provider was suitable. Tried [%s].".format(
          contextFactories.map(_.getClass.getName).mkString(", ")))
  }

  /**
   * @returns the protocol used to create SSLContext instances
   */
  def protocol() = defaultProtocol


  /**
   * @returns an SSLContext provisioned to be a client
   */
  private[this] def clientContext =
    SSLContext.getInstance(protocol())

  /**
   * Create a client. Always uses the default Sun JSSE implementation.
   */
  def client(): SSLContext = {
    val ctx = clientContext
    ctx.init(null, null, null)
    DefaultJSSEConfig(ctx)
    ctx
  }

  /**
   * Create a client with a trust manager that does not validate certificates.
   *
   * Obviously, this is insecure, but is included as it is useful for testing.
   */
  def clientWithoutCertificateValidation(): SSLContext = {
    val ctx = clientContext
    ctx.init(null, trustAllCertificates(), null)
    DefaultJSSEConfig(ctx)
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

    val chmod = Runtime.getRuntime.exec(Array(
      "chmod",
      "0700",
      dir.getAbsolutePath()))
    chmod.waitFor()

    if (chmod.exitValue() != 0) {
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
