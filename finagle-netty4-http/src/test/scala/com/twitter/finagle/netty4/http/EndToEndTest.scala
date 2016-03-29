package com.twitter.finagle.netty4.http

import com.twitter.conversions.time._
import com.twitter.conversions.storage._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.http.service.HttpResponseClassifier
import com.twitter.finagle.http.{Fields, Status, Response, Request}
import com.twitter.finagle._
import com.twitter.finagle.param.Stats
import com.twitter.finagle.service.{ResponseClass, FailureAccrualFactory}
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver, InMemoryStatsReceiver}
import com.twitter.finagle.tracing.Trace
import com.twitter.io.{Reader, Buf, Writer}
import com.twitter.util.{Await, Closable, Future, JavaTimer, Promise, Return, Throw, Time}
import java.io.{PrintWriter, StringWriter}
import java.net.InetSocketAddress
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner
import scala.language.reflectiveCalls

// this class is ported from c.t.finagle.http.EndToEndTest so
// we should maintain parity until the former is rm'd.
@RunWith(classOf[JUnitRunner])
class EndToEndTest extends FunSuite with BeforeAndAfter {
  var saveBase: Dtab = Dtab.empty
  private val statsRecv: InMemoryStatsReceiver = new InMemoryStatsReceiver()

  before {
    saveBase = Dtab.base
    Dtab.base = Dtab.read("/foo=>/bar; /baz=>/biz")
    statsRecv.clear()
  }

  after {
    Dtab.base = saveBase
    statsRecv.clear()
  }

  type HttpService = Service[Request, Response]
  type HttpTest = String => (HttpService => HttpService) => Unit

  def drip(w: Writer): Future[Unit] = w.write(buf("*")) before drip(w)
  def buf(msg: String): Buf = Buf.Utf8(msg)

  /**
   * Read `n` number of bytes from the bytestream represented by `r`.
   */
  def readNBytes(n: Int, r: Reader): Future[Buf] = {
    def loop(left: Buf): Future[Buf] = (n - left.length) match {
      case x if x > 0 =>
        r.read(x) flatMap {
          case Some(right) => loop(left concat right)
          case None => Future.value(left)
        }
      case _ => Future.value(left)
    }

    loop(Buf.Empty)
  }

  private def requestWith(status: Status): Request =
    Request(("statusCode", status.code.toString))

  private val statusCodeSvc = new HttpService {
    def apply(request: Request) = {
      val statusCode = request.getIntParam("statusCode", Status.BadRequest.code)
      Future.value(Response(Status.fromCode(statusCode)))
    }
  }

  def run(name: String)(tests: HttpTest*)(connect: HttpService => HttpService): Unit = {
    tests.foreach(t => t(name)(connect))
  }

  def standardErrors(name: String)(connect: HttpService => HttpService): Unit = {
    test(name + ": request uri too long") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response())
      }
      val client = connect(service)
      val request = Request("/" + "a" * 4096)
      val response = Await.result(client(request), 2.seconds)
      assert(response.status == Status.RequestURITooLong)
      client.close()
    }

    test(name + ": request header fields too large") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response())
      }
      val client = connect(service)
      val request = Request()
      request.headers().add("header", "a" * 8192)
      val response = Await.result(client(request), 2.seconds)
      assert(response.status == Status.RequestHeaderFieldsTooLarge)
      client.close()
    }

    test(name + ": with default client-side ResponseClassifier") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response())
      }
      val client = connect(service)

      Await.ready(client(requestWith(Status.Ok)), 1.second)
      assert(statsRecv.counters(Seq("client", "requests")) == 1)
      assert(statsRecv.counters(Seq("client", "success")) == 1)

      Await.ready(client(requestWith(Status.ServiceUnavailable)), 1.second)
      assert(statsRecv.counters(Seq("client", "requests")) == 2)
      // by default any `Return` is a successful response.
      assert(statsRecv.counters(Seq("client", "success")) == 2)

      client.close()
    }


    test(name + ": unhandled exceptions are converted into 500s") {
      val service = new HttpService {
        def apply(request: Request) = Future.exception(new IllegalArgumentException("bad news"))
      }

      val client = connect(service)
      val response = Await.result(client(Request()), 2.seconds)
      assert(response.status == Status.InternalServerError)
      client.close()
    }

    test(name + ": return 413s for requests with too large payloads") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response())
      }
      val client = connect(service)

      val tooBig = Request("/")
      tooBig.content = Buf.ByteArray.Owned(new Array[Byte](101))

      val justRight = Request("/")
      justRight.content = Buf.ByteArray.Owned(Array[Byte](100))

      assert(Await.result(client(justRight), 2.seconds).status == Status.Ok)
      assert(Await.result(client(tooBig)).status == Status.RequestEntityTooLarge)
      client.close()
    }
  }

  def standardBehaviour(name: String)(connect: HttpService => HttpService) {
    test(name + ": client stack observes max header size") {
      val service = new HttpService {
        def apply(req: Request) = {
          val res = Response()
          res.headerMap.put("Foo", ("*" * 8192) + "Bar: a")
          Future.value(res)
        }
      }
      val client = connect(service)
      // Whether this fails or not, which determined by configuration of max
      // header size in client configuration, there should definitely be no
      // "Bar" header.
      val hasBar = client(Request()).transform {
        case Throw(_) => Future.False
        case Return(res) =>
          val names = res.headerMap.keys
          Future.value(names.exists(_.contains("Bar")))
      }
      assert(!Await.result(hasBar, 2.seconds))
      client.close()
    }

    test(name + ": client sets content length") {
      val service = new HttpService {
        def apply(request: Request) = {
          val response = Response()
          val len = request.headerMap.get(Fields.ContentLength)
          response.contentString = len.getOrElse("")
          Future.value(response)
        }
      }
      val body = "hello"
      val client = connect(service)
      val req = Request()
      req.contentString = body
      val res = Await.result(client(req), 2.seconds)
      assert(res.contentString == body.length.toString)
      client.close()
    }

    test(name + ": echo") {
      val service = new HttpService {
        def apply(request: Request) = {
          val response = Response()
          response.contentString = request.uri
          Future.value(response)
        }
      }

      val client = connect(service)
      val response = client(Request("123"))
      assert(Await.result(response, 2.seconds).contentString == "123")
      client.close()
    }

    test(name + ": dtab") {
      val service = new HttpService {
        def apply(request: Request) = {
          val stringer = new StringWriter
          val printer = new PrintWriter(stringer)
          Dtab.local.print(printer)
          val response = Response() // todo: broken?
          response.contentString = stringer.toString
          Future.value(response)
        }
      }

      val client = connect(service)

      Dtab.unwind {
        Dtab.local ++= Dtab.read("/a=>/b; /c=>/d")

        val res = Await.result(client(Request("/")), 2.seconds)
        assert(res.contentString == "Dtab(2)\n\t/a => /b\n\t/c => /d\n")
      }

      client.close()
    }

    test(name + ": (no) dtab") {
      val service = new HttpService {
        def apply(request: Request) = {
          val stringer = new StringWriter

          val response = Response() // todo: broken?
          response.contentString = "%d".format(Dtab.local.length)
          Future.value(response)
        }
      }

      val client = connect(service)

      val res = Await.result(client(Request("/")), 2.seconds)
      assert(res.contentString == "0")

      client.close()
    }

    test(name + ": context") {
      val writtenDeadline = Deadline.ofTimeout(5.seconds)
      val service = new HttpService {
        def apply(request: Request) = {
          val deadline = Deadline.current.get
          assert(deadline.deadline == writtenDeadline.deadline)
          val response = Response() // todo: broken?
          Future.value(response)
        }
      }

      Contexts.broadcast.let(Deadline, writtenDeadline) {
        val req = Request()
        val client = connect(service)
        val res = Await.result(client(Request("/")), 2.seconds)
        assert(res.status == Status.Ok)
        client.close()
      }
    }

    test(name + ": streaming response to non-streaming client") {
      def service(r: Reader) = new HttpService {
        def apply(request: Request) = {
          val response = Response()
          response.setChunked(true)
          response.writer.write(buf("hello")) before
          response.writer.write(buf("world")) before
          response.close()
          Future.value(response)
        }
      }

      val writer = Reader.writable()
      val client = connect(service(writer))
      val r = client(Request())
      val response = Await.result(r, 5.seconds)
      assert(response.contentString == "helloworld")
      client.close()
    }

    test(name + ": client abort") {
      import com.twitter.conversions.time._
      val timer = new JavaTimer
      val promise = new Promise[Response]
      val service = new HttpService {
        def apply(request: Request) = promise
      }
      val client = connect(service)
      client(Request())
      Await.ready(timer.doLater(20.milliseconds) {
        Await.ready(client.close(), 2.seconds)
        intercept[CancelledRequestException] {
          promise.isInterrupted match {
            case Some(intr) => throw intr
            case _ =>
          }
        }
      })
    }

    test(name + ": measure payload size") {
      val service = new HttpService {
        def apply(request: Request) = {
          val rep = Response()
          rep.content = request.content.concat(request.content)

          Future.value(rep)
        }
      }

      val client = connect(service)
      val req = Request()
      req.content = Buf.Utf8("." * 10)
      Await.ready(client(req), 2.seconds)

      assert(statsRecv.stat("client", "request_payload_bytes")() == Seq(10.0f))
      assert(statsRecv.stat("client", "response_payload_bytes")() == Seq(20.0f))
      assert(statsRecv.stat("server", "request_payload_bytes")() == Seq(10.0f))
      assert(statsRecv.stat("server", "response_payload_bytes")() == Seq(20.0f))
      client.close()
    }
  }

  def streaming(name: String)(connect: HttpService => HttpService) {
    def service(r: Reader) = new HttpService {
      def apply(request: Request) = {
        val response = new Response {
          final val httpResponse = request.response.httpResponse
          override def reader = r
        }
        response.setChunked(true)
        Future.value(response)
      }
    }

    test(name + ": symmetric reader and getContent") {
      val s = Service.mk[Request, Response] { req =>
        val buf = Await.result(Reader.readAll(req.reader), 2.seconds)
        assert(buf == Buf.Utf8("hello"))
        assert(req.contentString == "hello")

        req.response.content = req.content
        Future.value(req.response)
      }
      val req = Request()
      req.contentString = "hello"
      req.headerMap.put("Content-Length", "5")
      val client = connect(s)
      val res = Await.result(client(req), 2.seconds)

      val buf = Await.result(Reader.readAll(res.reader), 2.seconds)
      assert(buf == Buf.Utf8("hello"))
      assert(res.contentString == "hello")
    }

    test(name + ": stream") {
      val writer = Reader.writable()
      val client = connect(service(writer))
      val reader = Await.result(client(Request()), 2.seconds).reader
      Await.result(writer.write(buf("hello")), 2.seconds)
      assert(Await.result(readNBytes(5, reader), 2.seconds) == Buf.Utf8("hello"))
      Await.result(writer.write(buf("world")), 2.seconds)
      assert(Await.result(readNBytes(5, reader), 2.seconds) == Buf.Utf8("world"))
      client.close()
    }

    test(name + ": transport closure propagates to request stream reader") {
      val p = new Promise[Buf]
      val s = Service.mk[Request, Response] { req =>
        p.become(Reader.readAll(req.reader))
        Future.value(Response())
      }
      val client = connect(s)
      val req = Request()
      req.setChunked(true)
      Await.result(client(req), 2.seconds)
      client.close()
      intercept[ChannelClosedException] { Await.result(p, 2.seconds) }
    }

    test(name + ": transport closure propagates to request stream producer") {
      val s = Service.mk[Request, Response] { _ => Future.value(Response()) }
      val client = connect(s)
      val req = Request()
      req.setChunked(true)
      client(req)
      client.close()
      intercept[Reader.ReaderDiscarded] { Await.result(drip(req.writer), 5.seconds) }
    }

    test(name + ": request discard terminates remote stream producer") {
      val s = Service.mk[Request, Response] { req =>
        val res = Response()
        res.setChunked(true)
        def go = for {
          Some(c) <- req.reader.read(Int.MaxValue)
          _  <- res.writer.write(c)
          _  <- res.close()
        } yield ()
        // discard the reader, which should terminate the drip.
        go ensure req.reader.discard()

        Future.value(res)
      }

      val client = connect(s)
      val req = Request()
      req.setChunked(true)
      val resf = client(req)

      Await.result(req.writer.write(buf("hello")), 2.seconds)

      val contentf = resf flatMap { res => Reader.readAll(res.reader) }
      assert(Await.result(contentf, 2.seconds) == Buf.Utf8("hello"))

      // drip should terminate because the request is discarded.
      intercept[Reader.ReaderDiscarded] { Await.result(drip(req.writer), 2.seconds) }
    }

    test(name + ": client discard terminates stream and frees up the connection") {
      val s = new Service[Request, Response] {
        var rep: Response = null

        def apply(req: Request) = {
          rep = Response()
          rep.setChunked(true)

          // Make sure the body is fully read.
          // Then we hang forever.
          val body = Reader.readAll(req.reader)

          Future.value(rep)
        }
      }

      val client = connect(s)
      val rep = Await.result(client(Request()), 10.seconds)
      assert(s.rep != null)
      rep.reader.discard()

      s.rep = null

      // Now, make sure the connection doesn't clog up.
      Await.result(client(Request()), 10.seconds)
      assert(s.rep != null)
    }

    test(name + ": two fixed-length requests") {
      val svc = Service.mk[Request, Response] { _ => Future.value(Response()) }
      val client = connect(svc)
      Await.result(client(Request()), 2.seconds)
      Await.result(client(Request()), 2.seconds)
      client.close()
    }

    test(name +": does not measure payload size") {
      val svc = Service.mk[Request, Response] { _ => Future.value(Response()) }
      val client = connect(svc)
      Await.result(client(Request()), 2.seconds)

      assert(statsRecv.stat("client", "request_payload_bytes")() == Nil)
      assert(statsRecv.stat("client", "response_payload_bytes")() == Nil)
      assert(statsRecv.stat("server", "request_payload_bytes")() == Nil)
      assert(statsRecv.stat("server", "response_payload_bytes")() == Nil)
      client.close()
    }
  }

  def tracing(name: String)(connect: HttpService => HttpService) {
    test(name + ": trace") {
      var (outerTrace, outerSpan) = ("", "")

      val inner = connect(new HttpService {
        def apply(request: Request) = {
          val response = Response() // todo: broken?
          response.contentString = Seq(
            Trace.id.traceId.toString,
            Trace.id.spanId.toString,
            Trace.id.parentId.toString
          ).mkString(".")
          Future.value(response)
        }
      })

      val outer = connect(new HttpService {
        def apply(request: Request) = {
          outerTrace = Trace.id.traceId.toString
          outerSpan = Trace.id.spanId.toString
          inner(request)
        }
      })

      val response = Await.result(outer(Request()), 2.seconds)
      val Seq(innerTrace, innerSpan, innerParent) =
        response.contentString.split('.').toSeq
      assert(innerTrace == outerTrace, "traceId")
      assert(outerSpan == innerParent, "outer span vs inner parent")
      assert(innerSpan != outerSpan, "inner (%s) vs outer (%s) spanId".format(innerSpan, outerSpan))

      outer.close()
      inner.close()
    }
  }

// CSL-2587 - blocked on client builder support for netty4-config
//  run("ClientBuilder")(standardErrors, standardBehaviour) {
//    service =>
//      val server = ServerBuilder()
//        .codec(com.twitter.finagle.http.Http().maxRequestSize(100.bytes))
//        .reportTo(statsRecv)
//        .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
//        .name("server")
//        .build(service)
//
//      val client = ClientBuilder()
//        .stack(http.Http.client)
//        .reportTo(statsRecv)
//        .hosts(Seq(server.boundAddress.asInstanceOf[InetSocketAddress]))
//        .hostConnectionLimit(1)
//        .name("client")
//        .build()
//
//      new ServiceProxy(client) {
//        override def close(deadline: Time) =
//          Closable.all(client, server).close(deadline)
//      }
//  }

  run("Client/Server")(standardErrors, standardBehaviour, tracing) {
    service =>
      val server = com.twitter.finagle.Http.server
        .withLabel("server")
        .configured(Stats(statsRecv))
        .withMaxRequestSize(100.bytes)
        .serve("localhost:*", service)
      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
      val client = com.twitter.finagle.netty4.http.Http.client
        .configured(Stats(statsRecv))
        .newService("%s:%d".format(addr.getHostName, addr.getPort), "client")

      new ServiceProxy(client) {
        override def close(deadline: Time) =
          Closable.all(client, server).close(deadline)
      }
  }

// CSL-2587 - blocked on client builder support for netty4-config
//  run("ClientBuilder (streaming)")(streaming) {
//    service =>
//      val server = ServerBuilder()
//        .codec(com.twitter.finagle.http.Http().streaming(true))
//        .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
//        .name("server")
//        .build(service)
//
//      val client = ClientBuilder()
//        .stack(http.Http.client.withStreaming(true))
//        .hosts(Seq(server.boundAddress.asInstanceOf[InetSocketAddress]))
//        .hostConnectionLimit(1)
//        .name("client")
//        .build()
//
//      new ServiceProxy(client) {
//        override def close(deadline: Time) =
//          Closable.all(client, server).close(deadline)
//      }
//  }

// CSL-2587 - blocked on client builder support for netty4-config
//  run("ClientBuilder (tracing)")(tracing) {
//    service =>
//      val server = ServerBuilder()
//        .codec(com.twitter.finagle.http.Http().enableTracing(true))
//        .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
//        .name("server")
//        .build(service)
//
//      val client = ClientBuilder()
//        .stack(http.Http.client)
////        .codec(Http().enableTracing(true))
//        .hosts(Seq(server.boundAddress.asInstanceOf[InetSocketAddress]))
//        .hostConnectionLimit(1)
//        .name("client")
//        .build()
//
//      new ServiceProxy(client) {
//        override def close(deadline: Time) =
//          Closable.all(client, server).close(deadline)
//      }
//  }

  // use 1 less than the requeue limit so that we trigger failure accrual
  // before we run into the requeue limit.
  private val failureAccrualFailures = 19

  def status(name: String)(connect: (HttpService, StatsReceiver, String) => (HttpService)): Unit = {
    test(name + ": Status.busy propagates along the Stack") {
      val st = new InMemoryStatsReceiver
      val clientName = "http"
      val failService = new HttpService {
        def apply(req: Request): Future[Response] =
          Future.exception(Failure.rejected("unhappy"))
      }

      val client = connect(failService, st, clientName)
      intercept[Exception](Await.result(client(Request()), 2.seconds))

      assert(st.counters(Seq(clientName, "failure_accrual", "removals")) == 1)
      assert(st.counters(Seq(clientName, "retries", "requeues")) == failureAccrualFailures - 1)
      assert(st.counters(Seq(clientName, "failures", "restartable")) == failureAccrualFailures)
      client.close()
    }
  }

  status("Client/Server") {
    (service, st, name) =>
      val server = com.twitter.finagle.Http.serve(new InetSocketAddress(0), service)
      val client = com.twitter.finagle.netty4.http.Http.client
        .configured(Stats(st))
        .configured(FailureAccrualFactory.Param(failureAccrualFailures, () => 1.minute))
        .newService(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])), name)

      new ServiceProxy(client) {
        override def close(deadline: Time) =
          Closable.all(client, server).close(deadline)
      }
  }

  test("Client-side ResponseClassifier based on status code") {
    val classifier = HttpResponseClassifier {
      case (_, r: Response) if r.status == Status.ServiceUnavailable =>
        ResponseClass.NonRetryableFailure
    }

    val server = com.twitter.finagle.Http.server
      .configured(param.Stats(NullStatsReceiver))
      .serve("localhost:*", statusCodeSvc)
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = com.twitter.finagle.netty4.http.Http.client
      .configured(param.Stats(statsRecv))
      .withResponseClassifier(classifier)
      .newService("%s:%d".format(addr.getHostName, addr.getPort), "client")

    Await.ready(client(requestWith(Status.Ok)), 1.second)
    assert(statsRecv.counters(Seq("client", "requests")) == 1)
    assert(statsRecv.counters(Seq("client", "success")) == 1)

    Await.ready(client(requestWith(Status.ServiceUnavailable)), 1.second)
    assert(statsRecv.counters(Seq("client", "requests")) == 2)
    assert(statsRecv.counters(Seq("client", "success")) == 1)

    client.close()
    server.close()
  }

  test("server-side ResponseClassifier based on status code") {
    val classifier = HttpResponseClassifier {
      case (_, r: Response) if r.status == Status.ServiceUnavailable =>
        ResponseClass.NonRetryableFailure
    }

    val server = com.twitter.finagle.Http.server
      .withResponseClassifier(classifier)
      .withStatsReceiver(statsRecv)
      .withLabel("server")
      .serve("localhost:*", statusCodeSvc)
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = com.twitter.finagle.netty4.http.Http.client
      .newService("%s:%d".format(addr.getHostName, addr.getPort), "client")

    Await.ready(client(requestWith(Status.Ok)), 1.second)
    assert(statsRecv.counters(Seq("server", "requests")) == 1)
    assert(statsRecv.counters(Seq("server", "success")) == 1)

    Await.ready(client(requestWith(Status.ServiceUnavailable)), 1.second)
    assert(statsRecv.counters(Seq("server", "requests")) == 2)
    assert(statsRecv.counters(Seq("server", "success")) == 1)
    assert(statsRecv.counters(Seq("server", "failures")) == 1)

    client.close()
    server.close()
  }

  test("codec should require a message size be less than 2Gb") {
    intercept[IllegalArgumentException] {
      com.twitter.finagle.netty4.http.Http.client.withMaxRequestSize(3000.megabytes)
    }
    intercept[IllegalArgumentException] {
      com.twitter.finagle.netty4.http.Http.client.withMaxResponseSize(3000.megabytes)
    }
  }
}
