package com.twitter.finagle.http

import com.twitter.conversions.StorageUnitOps._
import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http2.param.InitialWindowSize
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle.{Http, ListeningServer, Service}
import com.twitter.io.{Buf, BufReader}
import com.twitter.util.{Await, Awaitable, Duration, Future, Memoize, StorageUnit}
import java.net.InetSocketAddress
import org.scalatest.funsuite.AnyFunSuite

/**
 * A permutated set of client/server/proxy tests of varying large response sizes to
 * verify client/server/streaming configurations behave as expected.
 */
class ClientServerIntegrationTest extends AnyFunSuite {

  private[this] def await[T](awaitable: Awaitable[T], timeout: Duration = 1.second): T =
    Await.result(awaitable, timeout)

  /**
   * Use [[clnt]] to make a request and fully read the response from the server.
   * This convenience method handles the concerns of reading the response whether it
   * is fully buffered or streamed. The [[expectedSize]] will be compared via an [[assert]]
   * and an exception will be thrown if the expected size does not match the bytes read from
   * the response.
   *
   * @param clnt Finagle HTTP Service[Request, Response]
   * @param timeout The duration in seconds to allow for the full response to be read
   *                Note: For streaming responses, up to 2x [[timeout]] may be allowed due to
   *                      the nature of obtaining the stream data from a returned [[Response]].
   * @param expectedSize The expected size of the fully read response bytes
   * @return The fully read [[Response]]
   */
  private[this] def read(
    clnt: Service[Request, Response],
    timeout: Duration = 1.second,
    expectedSize: StorageUnit
  ): Response = {
    val rep = await(clnt(Request()), timeout)

    val buf = await(BufReader.readAll(rep.reader), timeout)
    assert(buf.length == expectedSize.inBytes)
    if (!rep.isChunked) assert(rep.contentLength == Some(expectedSize.inBytes))

    rep
  }

  /*
   * We want to create a large file server scenario to test how
   * a client handles buffered vs streamed responses for known content lengths
   */

  // create a server that simply proxies on behalf of a client
  private[this] def serverWithClient(
    destination: ListeningServer,
    useHttp2: Boolean,
    streamLimit: StorageUnit = 2.megabytes,
    initialWindowSize: InitialWindowSize = InitialWindowSize(None)
  ): ListeningServer = {
    val clnt: Service[Request, Response] = client(
      server = destination,
      useHttp2 = true, // we will default to using HTTP/2 for the client and let destination control
      streamLimit = streamLimit,
      initialWindowSize = initialWindowSize
    )

    var server = Http.server
      .withStreaming(streamLimit)
    server = if (useHttp2) server.withHttp2 else server.withNoHttp2
    server.serve("localhost:*", clnt)
  }

  // memoize generated content for a server's response based on the content size
  private[this] val genBytes: Int => Buf = Memoize[Int, Buf] { size =>
    val bytes: Array[Byte] = new Array[Byte](size)
    scala.util.Random.nextBytes(bytes)
    Buf.ByteArray(bytes: _*)
  }

  /**
   * Create an HTTP Server that generates random content of a specified size
   *
   * @param useHttp2 enable HTTP/2 on the server if true, disable otherwise
   * @param contentLength The size of the response to generate for any request
   * @param streamLimit The maximum response size before the response becomes a streaming response
   * @return The [[ListeningServer]] that serves the generated content
   */
  private[this] def server(
    useHttp2: Boolean,
    contentLength: StorageUnit,
    streamLimit: StorageUnit = 2.megabytes
  ): ListeningServer = {

    val content = genBytes(contentLength.inBytes.toInt)

    val svc = Service.mk[Request, Response] { _ =>
      val rep = Response()
      rep.content = content
      Future.value(rep)
    }

    var server = Http.server
      .withStreaming(streamLimit)
    server = if (useHttp2) server.withHttp2 else server.withNoHttp2
    server.serve("localhost:*", svc)
  }

  /**
   *
   * @param server The destination server to send requests to
   * @param useHttp2 Whether the client will attempt to use HTTP/2 for requests
   * @param streamLimit The `fixedLengthStreamAfter` size of the HTTP client
   * @param initialWindowSize The [[InitialWindowSize]] setting for an HTTP/2 enabled client
   *                          Note: does nothing if the client is HTTP/1.1 (i.e. useHttp2=false)
   * @return
   */
  private[this] def client(
    server: ListeningServer,
    useHttp2: Boolean,
    streamLimit: StorageUnit = 2.megabytes,
    initialWindowSize: InitialWindowSize = InitialWindowSize(None)
  ): Service[Request, Response] = {
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    var clnt = Http.client
      .withStreaming(streamLimit)
      .configured(FailureDetector.Param(FailureDetector.NullConfig)) // disable PingFailureDetection
      .configured(initialWindowSize)

    clnt = if (useHttp2) clnt.withHttp2 else clnt.withNoHttp2
    clnt.newService(s"inet!${addr.getHostName}:${addr.getPort}", "client")
  }

  private[ClientServerIntegrationTest] case class ServerConfig(
    useHttp2: Boolean,
    contentLength: StorageUnit,
    streamLimit: StorageUnit = 2.megabytes)

  private[ClientServerIntegrationTest] case class ClientConfig(
    useHttp2: Boolean,
    streamLimit: StorageUnit = 2.megabytes,
    initialWindowSize: InitialWindowSize = InitialWindowSize(None))

  private[ClientServerIntegrationTest] def initialWindowSizeLabel(cfg: ClientConfig): String =
    cfg match {
      case ClientConfig(useHttp2, _, iws) if useHttp2 && iws.initialWindowSize.isDefined =>
        s"initialWindowSize=${iws.initialWindowSize.get.inKilobytes}KB "
      case _ => ""
    }

  private[ClientServerIntegrationTest] case class TestSpec(
    srvCfg: ServerConfig,
    clntCfg: ClientConfig,
    proxyCfg: Option[ClientConfig] = None) {

    def run(): Unit = {
      def streamingLabel(srvCfg: ServerConfig, clntCfg: ClientConfig): String =
        (srvCfg.streamLimit, clntCfg.streamLimit) match {
          case (srvStream, clntStream) if srvStream == clntStream =>
            if (srvStream <= srvCfg.contentLength) "streaming" else "no streaming"
          case (srvStream, clntStream) if srvStream > clntStream =>
            if (clntStream <= srvCfg.contentLength) "client only streaming" else "no streaming"
          case _ => "server forced streaming"
        }

      val proxyLabel = proxyCfg match {
        case Some(cfg) =>
          val proxyProto = if (cfg.useHttp2) "HTTP/2" else "HTTP/1.1"
          val proxyStreaming = streamingLabel(srvCfg, cfg)
          val initialWindowSize = initialWindowSizeLabel(cfg)
          s"# proxy ($proxyProto - $proxyStreaming) $initialWindowSize"
        case _ => ""
      }
      val srvProto = if (srvCfg.useHttp2) "HTTP/2" else "HTTP/1.1"
      val clntProto = if (clntCfg.useHttp2) "HTTP/2" else "HTTP/1.1"
      val streamLabel = streamingLabel(srvCfg, clntCfg)
      val initialWindowSize = initialWindowSizeLabel(clntCfg)

      test(
        s"$srvProto Server and $clntProto Client (${srvCfg.contentLength.inKilobytes} KB responses) $initialWindowSize- $streamLabel$proxyLabel") {
        val srv = server(
          useHttp2 = srvCfg.useHttp2,
          contentLength = srvCfg.contentLength,
          streamLimit = srvCfg.streamLimit)
        val proxy: Option[ListeningServer] = proxyCfg.map { cfg =>
          serverWithClient(
            destination = srv,
            useHttp2 = cfg.useHttp2,
            streamLimit = cfg.streamLimit,
            initialWindowSize = cfg.initialWindowSize)
        }

        val dst = proxy.getOrElse(srv)
        val clnt = client(
          server = dst,
          useHttp2 = clntCfg.useHttp2,
          streamLimit = clntCfg.streamLimit,
          initialWindowSize = clntCfg.initialWindowSize)

        try {
          (1 to 10) foreach { _ =>
            val rep = read(clnt = clnt, timeout = 2.seconds, expectedSize = srvCfg.contentLength)
            val isClientStreaming = srvCfg.contentLength > clntCfg.streamLimit
            assert(
              rep.isChunked == isClientStreaming,
              s"Client should be streaming? $isClientStreaming")
          }
        } finally {
          val toClose = proxy match {
            case Some(p) => Seq(clnt.close(), p.close(), srv.close())
            case _ => Seq(clnt.close(), srv.close())
          }
          Await.all(toClose, 2.seconds)
        }
      }
    }
  }

  private[this] val tests =
    Seq(64.kilobytes, 512.kilobytes, 1.megabytes, 2.megabytes, 5.megabytes, 10.megabytes).flatMap {
      size =>
        Seq(
          // default HTTP/2
          TestSpec(
            ServerConfig(true, size),
            ClientConfig(true)
          ),
          // default HTTP/2 proxy
          TestSpec(
            ServerConfig(true, size),
            ClientConfig(true),
            Some(ClientConfig(true))
          ),
          // default h1 client, h2 proxy + server
          TestSpec(
            ServerConfig(true, size),
            ClientConfig(false),
            Some(ClientConfig(true))
          ),
          // default h1 client + proxy, h2 server
          TestSpec(
            ServerConfig(true, size),
            ClientConfig(false),
            Some(ClientConfig(false))
          ),
          // default h1 client + proxy + server
          TestSpec(
            ServerConfig(false, size),
            ClientConfig(false),
            Some(ClientConfig(false))
          ),
          // h2 client + server (client forced streaming)
          TestSpec(
            ServerConfig(true, size),
            ClientConfig(true, streamLimit = size * .25)
          ),
          // h1 client + server (client forced streaming)
          TestSpec(
            ServerConfig(false, size),
            ClientConfig(false, streamLimit = size * .25)
          ),
          // h2 client + proxy + server (proxy forced streaming)
          TestSpec(
            ServerConfig(true, size),
            ClientConfig(true),
            Some(ClientConfig(true, streamLimit = size * .25))
          ),
          // h1 client, h2 proxy + server (proxy forced streaming)
          TestSpec(
            ServerConfig(true, size),
            ClientConfig(false),
            Some(ClientConfig(true, streamLimit = size * .25))
          ),
          // h2 client + server (server forced streaming)
          TestSpec(
            ServerConfig(true, size, streamLimit = size * .25),
            ClientConfig(true)
          ),
          // h1 client + server (server forced streaming)
          TestSpec(
            ServerConfig(false, size, streamLimit = size * .25),
            ClientConfig(false)
          ),
          // HTTP/2 server + client with non-default window size
          TestSpec(
            ServerConfig(true, size),
            ClientConfig(true, initialWindowSize = InitialWindowSize(Some(size)))
          ),
          // HTTP/2 server + client with non-default window size + proxy w/ default window
          TestSpec(
            ServerConfig(true, size),
            ClientConfig(true, initialWindowSize = InitialWindowSize(Some(size))),
            Some(ClientConfig(true))
          ),
          // HTTP/2 server + client with non-default window size + proxy w/ non-default window
          TestSpec(
            ServerConfig(true, size),
            ClientConfig(true, initialWindowSize = InitialWindowSize(Some(size))),
            Some(ClientConfig(true, initialWindowSize = InitialWindowSize(Some(size))))
          ),
          // HTTP/1.1 client + HTTP/2 server, proxy w/ non-default window
          TestSpec(
            ServerConfig(true, size),
            ClientConfig(false),
            Some(ClientConfig(true, initialWindowSize = InitialWindowSize(Some(size))))
          )
        )
    }

  tests.foreach(_.run())

}
