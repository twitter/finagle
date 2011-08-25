package com.twitter.finagle.ssl

import java.io._
import java.security.cert.X509Certificate
import java.security.cert.{Certificate, CertificateFactory}
import java.security.spec._
import java.security.{KeyFactory, KeyStore, Security, Provider, PrivateKey}
import java.util.Random
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.{Level, Logger}
import javax.net.ssl._

import scala.collection.JavaConversions._
import scala.collection.mutable.{Map => MutableMap}
import scala.util.control.Breaks._

import org.jboss.netty.channel.{Channel, ChannelHandlerContext, ChannelLocal,
                                MessageEvent, SimpleChannelHandler}

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

  private[this] def readCertAndKey(certificatePath: String, keyPath: String): EncodedCertAndKey =
    new EncodedCertAndKey(
      readFileToBytes(certificatePath),
      readFileToBytes(keyPath))

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

  def apply(certificatePath: String, keyPath: String): Array[KeyManager] =
    importKeystore(readCertAndKey(certificatePath: String, keyPath: String))
}

object Ssl {
  private[this] val contextCacheEnabled = true
  private[this] val log = Logger.getLogger(getClass.getName)
  private[this] val defaultProtocol = "TLS"

  private[this] def fileMustExist(path: String) =
    require(new File(path).exists(), "File '%s' does not exist.".format(path))

  object OpenSSL {
    type MapOfStrings = java.util.Map[java.lang.String, java.lang.String]

    private[this] def classNamed(name: String): Class[_] =
      Class.forName(name)

    // For flagging global initialization of APR and OpenSSL
    private[this] val initializedLibrary = new AtomicBoolean(false)

    private[this] var bufferPool: AnyRef = null
    private[this] val defaultCiphers = "AES128-SHA:RC4:AES:!ADH:!aNULL:!DH:!EDH:!PSK:!ECDH:!eNULL:!LOW:!SSLv2:!EXP:!NULL"

    class Linker {
      val classBase = "org.apache.tomcat.jni."
      val aprClass = classNamed(classBase + "Library")
      val aprInitMethod = aprClass.getMethod("initialize", classOf[String])
      val sslClass = classNamed(classBase + "SSL")
      val sslInitMethod = sslClass.getMethod("initialize", classOf[String])

      // OpenSSLEngine-specific configuration classes
      val bufferPoolClass    = classNamed(classBase + "ssl.DirectBufferPool")
      val bufferPoolCtor     = bufferPoolClass.getConstructor(classOf[Int])
      val bufferPoolCapacity = 5000.asInstanceOf[AnyRef]

      val configurationClass = classNamed(classBase + "ssl.SSLConfiguration")
      val configurationCtor  = configurationClass.getConstructor(classOf[MapOfStrings])

      val contextHolderClass = classNamed(classBase + "ssl.SSLContextHolder")
      val contextHolderCtor  = contextHolderClass.getConstructor(configurationClass)

      val sslEngineClass     = classNamed(classBase + "ssl.OpenSSLEngine")
      val sslEngineCtor      = sslEngineClass.getConstructor(contextHolderClass, bufferPoolClass)


      if (initializedLibrary.compareAndSet(false, true)) {
        aprInitMethod.invoke(aprClass, null)
        sslInitMethod.invoke(sslClass, null)
        bufferPool = bufferPoolCtor.newInstance(bufferPoolCapacity).asInstanceOf[AnyRef]
      }
    }

    private[this] val contextHolderCache: MutableMap[String, Object] = MutableMap.empty
    private[this] var linker: Linker = null

    def server(certificatePath: String, keyPath: String, caPath: String, ciphers: String): Option[SSLEngine] = {
      try {
        if (linker == null)
          linker = new Linker()
      } catch { case e: Exception =>
        log.warning("APR/OpenSSL could not be loaded: " + e.getClass().getName() + ": " + e.getMessage())
          e.printStackTrace()
          return None
        }

      def makeContextHolder = {
        val configMap = new java.util.HashMap[java.lang.String, java.lang.String]
        configMap.put("ssl.cert_path", certificatePath)
        configMap.put("ssl.key_path", keyPath)
        configMap.put("ssl.cipher_spec", Option(ciphers).getOrElse { defaultCiphers })

        if (caPath != null)
          configMap.put("ssl.ca_path", caPath)

        val config = linker.configurationCtor.newInstance(configMap.asInstanceOf[MapOfStrings])

        log.finest("OpenSSL context instantiated for certificate '%s'".format(certificatePath))

        linker.contextHolderCtor.newInstance(config.asInstanceOf[AnyRef]).asInstanceOf[AnyRef]
      }

      val key = "%s-%s-%s-%s".format(certificatePath, keyPath, caPath, ciphers)
      val contextHolder = if (contextCacheEnabled)
        contextHolderCache.getOrElseUpdate(certificatePath, makeContextHolder)
      else
        makeContextHolder

      val engine: SSLEngine = linker.sslEngineCtor.newInstance(contextHolder, bufferPool).asInstanceOf[SSLEngine]
      Some(engine)
    }
  }

  object JSSE {
    private[this] def protocol() = defaultProtocol
    private[this] val contextCache: MutableMap[String, SSLContext] = MutableMap.empty

    def server(certificatePath: String, keyPath: String): Option[SSLEngine] = {
      def makeContext: SSLContext = {
        fileMustExist(certificatePath)
        fileMustExist(keyPath)

        val context = SSLContext.getInstance(protocol())
        val kms = PEMEncodedKeyManager(certificatePath, keyPath)
        context.init(kms, null, null)

        log.finest("JSSE context instantiated for certificate '%s'".format(certificatePath))

        context
      }

      val key = "%s-%s".format(certificatePath, keyPath)
      val context: SSLContext =
        if (contextCacheEnabled)
          contextCache.getOrElseUpdate(key, makeContext)
        else
          makeContext


      Some(context.createSSLEngine())
    }

    /**
     * @returns the protocol used to create SSLContext instances
     */

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

    private[this] def clientContext =
      SSLContext.getInstance(protocol())

    private[this] def client(trustManagers: Array[TrustManager]): SSLEngine = {
      val ctx = clientContext
      ctx.init(null, trustManagers, null)
      ctx.createSSLEngine()
    }

    def client(): SSLEngine = client(null)
    def clientWithoutCertificateValidation(): SSLEngine = client(trustAllCertificates())
  }

  /**
   * Get a server engine, using the native OpenSSL provider if available.
   *
   * @param certificatePath The path to the PEM encoded certificate file
   * @param keyPath The path to the PEM encoded key file corresponding to the certificate
   * @param caCertPath The path to the optional PEM encoded CA certificate (use only for OpenSSL)
   * @param cipherSpec The cipher spec (use only for OpenSSL)
   * @throws RuntimeException if no provider could be initialized
   * @returns an SSLEngine
   */
  def server(certificatePath: String, keyPath: String, caCertPath: String, ciphers: String): SSLEngine = {
    val engine = OpenSSL.server(certificatePath, keyPath, caCertPath, ciphers).getOrElse {
      if (caCertPath != null)
        throw new RuntimeException("'CA Certificate' parameter has no effect; unsupported with JSSE")

      if (ciphers != null)
        throw new RuntimeException("'Ciphers' parameter has no effect; unsupported with JSSE")

      JSSE.server(certificatePath, keyPath).getOrElse {
        throw new RuntimeException("Could not create an SSLEngine")
      }
    }

    engine
  }


  /**
   * Get a client engine
   */
  def client(): SSLEngine = JSSE.client()

  /**
   * Get a client engine without certificate validation
   */
  def clientWithoutCertificateValidation(): SSLEngine = JSSE.clientWithoutCertificateValidation()
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
