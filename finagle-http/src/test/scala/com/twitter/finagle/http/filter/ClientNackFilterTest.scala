package com.twitter.finagle.http.filter

import com.twitter.finagle.http.{Fields, Method, Request, Response, Status}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.{Status => _, _}
import com.twitter.io.Buf
import com.twitter.util.{Await, Awaitable, Closable, Duration, Future}
import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicBoolean
import org.scalatest.funsuite.AnyFunSuite

class ClientNackFilterTest extends AnyFunSuite {

  private def await[T](t: Awaitable[T]): T =
    Await.result(t, Duration.fromSeconds(15))

  test("converts nacked requests into failures with the right flags") {
    def withHeaderService(key: String, value: String) =
      (new ClientNackFilter).andThen(Service.mk[Request, Response] { _ =>
        val resp = Response(Status.ServiceUnavailable)
        resp.headerMap.set(key, value)
        Future.value(resp)
      })

    def req = Request("/foo")

    { // Retry-After: 0 nack
      val retryAfterSvc = withHeaderService(Fields.RetryAfter, "0")
      val retryAfterFailure = intercept[Failure] { await(retryAfterSvc(req)) }
      assert(retryAfterFailure.isFlagged(FailureFlags.Retryable))
      assert(retryAfterFailure.isFlagged(FailureFlags.Rejected))
    }

    { // finagle retryable nack
      val nackSvc = withHeaderService(HttpNackFilter.RetryableNackHeader, "true")
      val retryableFailure = intercept[Failure] { await(nackSvc(req)) }
      assert(retryableFailure.isFlagged(FailureFlags.Retryable))
      assert(retryableFailure.isFlagged(FailureFlags.Rejected))
    }

    { // finagle non-retryable nack
      val nackSvc = withHeaderService(HttpNackFilter.NonRetryableNackHeader, "true")
      val nonRetryableFailure = intercept[Failure] { await(nackSvc(req)) }
      assert(!nonRetryableFailure.isFlagged(FailureFlags.Retryable))
      assert(nonRetryableFailure.isFlagged(FailureFlags.Rejected))
    }
  }

  test("Lets a regular request through") {
    new NackCtx(withStreaming = false) {
      def nackBody: Buf = Buf.Utf8("Non-chunked nack body")

      assert(await(client(Request("/foo"))).status == http.Status.Ok)
      assert(serverSr.counters(Seq("myservice", "nacks")) == 0)
      assert(clientSr.counters(Seq("http", "retries", "requeues")) == 0)

      // reuse connections
      assert(await(client(Request("/bar"))).status == http.Status.Ok)
      assert(clientSr.counters(Seq("http", "connects")) == 1)
      assert(serverSr.counters(Seq("myservice", "nacks")) == 0)

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

      // h2 sessions should remain at 1 and the connection should not sever
      assert(clientSr.counters(Seq("http", "connects")) == 1)
      assert(clientSr.gauges(Seq("http", "h2pool-sessions"))() == 1.0f)
      assert(clientSr.counters(Seq("http", "failures", "rejected")) == 1)
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

  test("multiple nack headers are not added if the request is retried") {
    val first = new AtomicBoolean(false)
    val service = new ClientNackFilter().andThen(Service.mk { req: Request =>
      if (first.compareAndSet(false, true)) {
        // Nack the first one
        Future.exception(Failure.rejected)
      } else {
        val resp = Response()
        resp.contentString =
          req.headerMap.getAll(HttpNackFilter.RetryableRequestHeader).length.toString
        Future.value(resp)
      }
    })

    val request = Request(method = Method.Post, uri = "/")
    request.contentString = "post"

    request.contentString
    val ex = intercept[FailureFlags[_]] { await(service(request)) }

    assert(ex.isFlagged(FailureFlags.Retryable))
    assert(request.headerMap.getAll(HttpNackFilter.RetryableRequestHeader).length == 1)

    // Do it again.
    val response = await(service(request))
    assert(request.headerMap.getAll(HttpNackFilter.RetryableRequestHeader).length == 1)
    assert(response.contentString == "1")
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
