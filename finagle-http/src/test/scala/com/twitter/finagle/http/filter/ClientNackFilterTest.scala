package com.twitter.finagle.http.filter

import com.twitter.finagle.http.{Method, Request, Response}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle._
import com.twitter.io.Buf
import com.twitter.util.{Await, Awaitable, Closable, Duration, Future}
import java.net.InetSocketAddress
import org.scalatest.FunSuite

class ClientNackFilterTest extends FunSuite {

  private def await[T](t: Awaitable[T]): T =
    Await.result(t, Duration.fromSeconds(15))

  test("Lets a regular request through") {
    new NackCtx(withStreaming = false) {
      def nackBody: Buf = Buf.Utf8("Non-chunked nack body")

      assert(await(client(Request("/foo"))).status == http.Status.Ok)
      assert(serverSr.counters.get(Seq("myservice", "nacks")).isEmpty)
      assert(clientSr.counters.get(Seq("http", "retries", "requeues")).isEmpty)

      // reuse connections
      assert(await(client(Request("/bar"))).status == http.Status.Ok)
      assert(clientSr.counters(Seq("http", "connects")) == 1)
      assert(serverSr.counters.get(Seq("myservice", "nacks")).isEmpty)

      closeCtx()
    }
  }

  test("Converts an aggregated Nack response") {
    new NackCtx(withStreaming = false) {
      def nackBody: Buf = Buf.Utf8("Non-chunked nack body")

      assert(await(client(request)).status == http.Status.Ok)
      assert(serverSr.counters(Seq("myservice", "nacks")) == 1)
      assert(clientSr.counters(Seq("http", "retries", "requeues")) == 1)

      // reuse connections
      assert(await(client(request)).status == http.Status.Ok)
      assert(clientSr.counters(Seq("http", "connects")) == 1)
      assert(serverSr.counters(Seq("myservice", "nacks")) == 1)

      closeCtx()
    }
  }

  test("swallows the body of a HttpNack if it happens to come as a chunked response") {
    new NackCtx(withStreaming = true) {
      def nackBody: Buf = Buf.Utf8("Chunked nack body")

      assert(await(client(request)).status == http.Status.Ok)
      assert(serverSr.counters(Seq("myservice", "nacks")) == 1)
      assert(clientSr.counters(Seq("http", "retries", "requeues")) == 1)

      // reuse connections
      assert(await(client(request)).status == http.Status.Ok)
      assert(clientSr.counters(Seq("http", "connects")) == 1)
      assert(serverSr.counters(Seq("myservice", "nacks")) == 1)

      closeCtx()
    }
  }

  test("fails on excessively large nack response") {
    new NackCtx(withStreaming = true) {
      def nackBody: Buf = Buf.Utf8("Very large" * 1024)

      assert(await(client(request)).status == http.Status.Ok)

      // Should have closed the connection on the first nack
      assert(clientSr.counters(Seq("http", "connects")) == 2)
      assert(serverSr.counters(Seq("myservice", "nacks")) == 1)

      closeCtx()
    }
  }

  test("always marks streaming requests as non-retryable") {
    new NackCtx(withStreaming = false) {
      def nackBody: Buf = Buf.Utf8("whatever")
      val f = intercept[Failure] {
        request.method(Method.Post)
        request.setChunked(true)
        request.writer.close() // don't really write anything, but the dispatcher cant know that...

        await(client(request))
      }

      assert(f.isFlagged(FailureFlags.Rejected))
      assert(!f.isFlagged(FailureFlags.Retryable))

      assert(clientSr.counters(Seq("http", "requests")) == 1)
      assert(serverSr.counters(Seq("myservice", "nacks")) == 1)

      closeCtx()
    }
  }

  test("adds the magic header to requests that aren't chunked") {
    val markerHeader = "finagle-had-retryable-request-header"
    val service = new ClientNackFilter().andThen(Service.mk { req: Request =>
      val resp = Response()

      if (req.headerMap.contains(HttpNackFilter.RetryableRequestHeader)) {
        resp.headerMap.add(markerHeader, "")
      }
      Future.value(resp)
    })

    val nonChunkedRequest = Request(method = Method.Post, uri = "/")
    nonChunkedRequest.contentString = "static"
    val nonChunkedResponse = await(service(nonChunkedRequest))
    assert(nonChunkedResponse.headerMap.contains(markerHeader))

    val chunkedRequest = Request(method = Method.Post, uri = "/")
    chunkedRequest.setChunked(true)
    val chunkedResponse = await(service(chunkedRequest))
    assert(!chunkedResponse.headerMap.contains(markerHeader))
  }

  // Scaffold for checking nack behavior
  private abstract class NackCtx(withStreaming: Boolean) {

    private val ChunkedNack = "/chunkednack"
    private val StdNack = "/stdnack"

    def nackBody: Buf
    val serverSr = new InMemoryStatsReceiver
    val clientSr = new InMemoryStatsReceiver
    @volatile var needsNack: Boolean = true
    val service: Service[Request, Response] = Service.mk { req: Request =>
      req.path match {
        case ChunkedNack if needsNack =>
          Future.value {
            needsNack = false
            // simulate a nack response with a chunked body by just sending a chunked body
            serverSr.scope("myservice").counter("nacks").incr()
            val resp = Response(status = HttpNackFilter.ResponseStatus)
            resp.headerMap.set(HttpNackFilter.RetryableNackHeader, "true")
            resp.setChunked(true)
            resp.writer
              .write(nackBody)
              .before(resp.writer.close())
            resp
          }

        case StdNack if needsNack =>
          Future.value {
            needsNack = false
            // simulate a nack response with a chunked body by just sending a chunked body
            serverSr.scope("myservice").counter("nacks").incr()
            val resp = Response(status = HttpNackFilter.ResponseStatus)
            resp.headerMap.set(HttpNackFilter.RetryableNackHeader, "true")
            resp.content = nackBody
            resp
          }

        case _ =>
          Future.value {
            val resp = Response()
            resp.contentString = "the body"
            resp
          }
      }
    }

    val server =
      Http.server
        .withStatsReceiver(serverSr)
        .withLabel("myservice")
        .withStreaming(true)
        .serve(new InetSocketAddress(0), service)
    val client =
      Http.client
        .withStatsReceiver(clientSr)
        .withStreaming(true)
        .newService(
          Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
          "http"
        )

    val request = Request(if (withStreaming) ChunkedNack else StdNack)

    // Close both the client and the server
    def closeCtx(): Unit = await(Closable.all(client, server).close())
  }
}
