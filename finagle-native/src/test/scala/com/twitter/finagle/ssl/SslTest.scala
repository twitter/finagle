package com.twitter.finagle.ssl

import com.twitter.finagle.Service
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.http.Http
import com.twitter.io.TempFile
import com.twitter.util.{Await, Future, NonFatal}
import java.io.File
import java.net.{InetAddress, InetSocketAddress}
import org.jboss.netty.buffer._
import org.jboss.netty.handler.codec.http._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SslTest extends FunSuite {
  val certChainInput = new CertChainInput(
    "setup-chain",                 // directory that contains the files below
    "setupCA.sh",
    "makecert.sh",
    "openssl-intermediate.conf",
    "openssl-root.conf"
  )

  // before we run any tests, construct the chain
  try {
    // would prefer to have an abstraction for what's below, but
    // Shell.run doesn't give you back the process
    certChainInput.setupCAFile.setExecutable(true)
    certChainInput.makeCertFile.setExecutable(true)
    // this process requires an openssl executable
    val process = Runtime.getRuntime.exec(
      Array[String](
        certChainInput.setupCAPath,
        certChainInput.makeCertPath,
        certChainInput.openSSLIntConfPath,
        certChainInput.openSSLRootConfPath
      ), // command
      null, // null == inherit the environment of the current process
      certChainInput.setupCADir // working dir
    )
    process.waitFor()
    assert(process.exitValue === 0)
  } catch {
    case e: java.io.IOException =>
      println("IOException: I/O error in running setupCA script: " +
        e.getMessage())
        throw e
    case NonFatal(e) => println("Unknown exception in running setupCA script: " +
        e.getMessage())
        throw e
  }

  // the chain should have generated the files below
  val certChain = new CertChainOutput(
    "test.example.com.chain",
    "test.example.com.cert",
    "test.example.com.key",
    "cacert.pem",
    certChainInput.setupCADirPath
  )

  // now let's run some tests
  test("be able to send and receive various sized content") {
    def makeContent(length: Int) = {
      val buf = ChannelBuffers.directBuffer(length)
      while (buf.writableBytes() > 0)
        buf.writeByte('Z')
      buf
    }

    val service = new Service[HttpRequest, HttpResponse] {
      def apply(request: HttpRequest) = Future {
        val requestedBytes = request.headers.get("Requested-Bytes")
          match {
          case s: String => s.toInt
          case _ => 17280
        }
        val response = new DefaultHttpResponse(
          HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
        Option(request.headers.get("X-Transport-Cipher")) foreach
        { cipher: String => response.headers.set("X-Transport-Cipher",
          cipher) }
        response.setContent(makeContent(requestedBytes))
        HttpHeaders.setContentLength(response, requestedBytes)

        response
      }
    }

    val codec =
      Http().annotateCipherHeader("X-Transport-Cipher")

    val server =
      ServerBuilder()
        .codec(codec)
        .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
        .tls(certChain.certPath, certChain.keyPath)
        .name("SSLServer")
        .build(service)

    def client =
      ClientBuilder()
        .name("http-client")
        .hosts(server.boundAddress)
        .codec(codec)
        .hostConnectionLimit(1)
        .tlsWithoutValidation()
        .build()

    def check(requestSize: Int, responseSize: Int) {
      val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
        HttpMethod.GET, "/")

      if (requestSize > 0) {
        request.setContent(makeContent(requestSize))
        HttpHeaders.setContentLength(request, requestSize)
      }

      if (responseSize > 0)
        request.headers.set("Requested-Bytes", responseSize)
      else
        request.headers.set("Requested-Bytes", 0)

      val response = Await.result(client(request))
      assert(response.getStatus === HttpResponseStatus.OK)
      assert(HttpHeaders.getContentLength(response) === responseSize)
      val content = response.getContent()

      assert(content.readableBytes() === responseSize)

      while (content.readableBytes() > 0) {
        assert(content.readByte() === 'Z')
      }

      val cipher = response.headers.get("X-Transport-Cipher")
      assert(cipher != "null")
    }

    check(   0 * 1024, 16   * 1024)
    check(  16 * 1024, 0    * 1024)
    check(1000 * 1024, 16   * 1024)
    check( 256 * 1024, 256  * 1024)
  }

  test("be able to validate a properly constructed authentication chain") {
    // ... spin up an SSL server ...
    val service = new Service[HttpRequest, HttpResponse] {
      def apply(request: HttpRequest) = Future {
        def makeContent(length: Int) = {
          val buf = ChannelBuffers.directBuffer(length)
          while (buf.writableBytes() > 0)
            buf.writeByte('Z')
          buf
        }
        val requestedBytes = request.headers.get("Requested-Bytes")
          match {
          case s: String => s.toInt
          case _ => 17280
        }
        val response = new DefaultHttpResponse(
          HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
        Option(request.headers.get("X-Transport-Cipher")) foreach {
          cipher: String =>
          response.headers.set("X-Transport-Cipher", cipher)
        }
        response.setContent(makeContent(requestedBytes))
        HttpHeaders.setContentLength(response, requestedBytes)

        response
      }
    }

    val codec =
      Http().annotateCipherHeader("X-Transport-Cipher")

    val server = ServerBuilder()
      .codec(codec)
      .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
      .tls(certChain.certPath,
        certChain.keyPath,
        certChain.validChainPath)
      .name("SSL server with valid certificate chain")
      .build(service)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]

    // ... then connect to that service using openssl and ensure that
    // the chain is correct
    val cmd = Array[String](
      "openssl", "s_client",
      "-connect",
      "localhost:" + addr.getPort.toString,
      "-CAfile",  certChain.rootCertOnlyPath, // cacert.pem
      "-verify", "9", "-showcerts"
    )

    try {
      // would prefer to have an abstraction for what's below, but
      // Shell.run doesn't give you back the process
      val process = Runtime.getRuntime.exec(cmd)
      process.getOutputStream.write("QUIT\n".getBytes)
      process.getOutputStream.close()

      process.waitFor()
      assert(process.exitValue === 0)

      // look for text "Verify return code: 0 (ok)" on stdout
      val out = process.getInputStream
      val outBuf = new Array[Byte](out.available)
      out.read(outBuf)
      val outBufStr = new String(outBuf)
      assert("Verify return code: 0 \\(ok\\)".r.findFirstIn(outBufStr) === Some("""Verify return code: 0 (ok)"""))
    } catch {
      case ex: java.io.IOException =>
        println("Test skipped: running openssl failed" +
          " (openssl executable might be absent?)")
    }
  }
}

// converts filenames to File objects and absolute-path filenames,
// which are then used as inputs to generate the certificate chain
class CertChainInput(
  setupCADirName: String,
  setupCAFilename: String,
  makeCertFilename: String,
  openSSLIntConfFilename: String,
  openSSLRootConfFilename: String
) {
  val root = File.separator + setupCADirName
  def fileInRoot(file: String) = root + File.separator + file
  val setupCADir                  =
    TempFile.fromResourcePath(getClass, root).getParentFile()
  val setupCAFile                 =
    TempFile.fromResourcePath(getClass, fileInRoot(setupCAFilename))
  val makeCertFile                =
    TempFile.fromResourcePath(getClass, fileInRoot(makeCertFilename))
  val openSSLIntConfFile          =
    TempFile.fromResourcePath(getClass, fileInRoot(openSSLIntConfFilename))
  val openSSLRootConfFile         =
    TempFile.fromResourcePath(getClass, fileInRoot(openSSLRootConfFilename))

  val setupCADirPath: String      = setupCADir.getAbsolutePath
  val setupCAPath: String         = setupCAFile.getAbsolutePath
  val makeCertPath: String        = makeCertFile.getAbsolutePath
  val openSSLIntConfPath: String  = openSSLIntConfFile.getAbsolutePath
  val openSSLRootConfPath: String = openSSLRootConfFile.getAbsolutePath
}

// converts filenames to File objects and absolute-path filenames
// for the generated certificate chain
class CertChainOutput(
  validChainFilename: String,
  certFilename: String,
  keyFilename: String,
  rootCertOnlyFilename: String,
  setupCADirPath: String
) {
  val validChainFile = new File(setupCADirPath, validChainFilename)
  val validChainPath = validChainFile.getAbsolutePath
  if (!validChainFile.canRead())
    throw new java.io.FileNotFoundException("Cannot read valid chain file")

  val certFile = new File(setupCADirPath, certFilename)
  val certPath = certFile.getAbsolutePath
  if (!certFile.canRead())
    throw new java.io.FileNotFoundException("Cannot read cert file")

  val keyFile = new File(setupCADirPath, keyFilename)
  val keyPath = keyFile.getAbsolutePath
  if (!keyFile.canRead())
    throw new java.io.FileNotFoundException("Cannot read key file")

  val rootCertOnlyFile = new File(setupCADirPath, rootCertOnlyFilename)
  val rootCertOnlyPath = rootCertOnlyFile.getAbsolutePath
  if (!rootCertOnlyFile.canRead())
    throw new java.io.FileNotFoundException("Cannot read root cert only file")
}
