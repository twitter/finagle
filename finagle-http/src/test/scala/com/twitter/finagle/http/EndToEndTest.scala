package com.twitter.finagle.http

import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.param.Stats
import com.twitter.finagle.service.FailureAccrualFactory
import com.twitter.finagle.stats.{InMemoryStatsReceiver, StatsReceiver}
import com.twitter.finagle.tracing.Trace
import com.twitter.io.{Buf, Reader, Writer}
import com.twitter.util.{Await, Closable, Future, JavaTimer, Promise, Return, Throw, Time}
import java.io.{PrintWriter, StringWriter}
import java.net.{InetAddress, InetSocketAddress}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends FunSuite with BeforeAndAfter {
  var saveBase: Dtab = Dtab.empty
  before {
    saveBase = Dtab.base
    Dtab.base = Dtab.read("/foo=>/bar; /baz=>/biz")
  }

  after {
    Dtab.base = saveBase
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
      val response = Await.result(client(request))
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
      val response = Await.result(client(request))
      assert(response.status == Status.RequestHeaderFieldsTooLarge)
      client.close()
    }

    test(name + ": unhandled exceptions are converted into 500s") {
      val service = new HttpService {
        def apply(request: Request) = Future.exception(new IllegalArgumentException("bad news"))
      }

      val client = connect(service)
      val response = Await.result(client(Request()))
      assert(response.status == Status.InternalServerError)
      client.close()
    }

    test(name + ": return 413s for requests with too large payloads") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response())
      }
      val client = connect(service)

      val tooBig = Request("/")
      tooBig.content = Buf.ByteArray.Owned(new Array[Byte](200))

      val justRight = Request("/")
      justRight.content = Buf.ByteArray.Owned(Array[Byte](100))

      assert(Await.result(client(tooBig)).status == Status.RequestEntityTooLarge)
      assert(Await.result(client(justRight)).status == Status.Ok)
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
      assert(!Await.result(hasBar))
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
      assert(Await.result(client(req)).contentString == body.length.toString)
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
      assert(Await.result(response).contentString == "123")
      client.close()
    }

    test(name + ": dtab") {
      val service = new HttpService {
        def apply(request: Request) = {
          val stringer = new StringWriter
          val printer = new PrintWriter(stringer)
          Dtab.local.print(printer)
          val response = Response(request)
          response.contentString = stringer.toString
          Future.value(response)
        }
      }

      val client = connect(service)

      Dtab.unwind {
        Dtab.local ++= Dtab.read("/a=>/b; /c=>/d")

        val res = Await.result(client(Request("/")))
        assert(res.contentString == "Dtab(2)\n\t/a => /b\n\t/c => /d\n")
      }

      client.close()
    }

    test(name + ": (no) dtab") {
      val service = new HttpService {
        def apply(request: Request) = {
          val stringer = new StringWriter

          val response = Response(request)
          response.contentString = "%d".format(Dtab.local.length)
          Future.value(response)
        }
      }

      val client = connect(service)

      val res = Await.result(client(Request("/")))
      assert(res.contentString == "0")

      client.close()
    }

    test(name + ": stream") {
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
      val response = Await.result(client(Request()))
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
        Await.ready(client.close())
        intercept[CancelledRequestException] {
          promise.isInterrupted match {
            case Some(intr) => throw intr
            case _ =>
          }
        }
      })
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
        val buf = Await.result(Reader.readAll(req.reader))
        assert(buf == Buf.Utf8("hello"))
        assert(req.contentString == "hello")

        req.response.content = req.content
        Future.value(req.response)
      }
      val req = Request()
      req.contentString = "hello"
      req.headerMap.put("Content-Length", "5")
      val client = connect(s)
      val res = Await.result(client(req))

      val buf = Await.result(Reader.readAll(res.reader))
      assert(buf == Buf.Utf8("hello"))
      assert(res.contentString == "hello")
    }

    test(name + ": stream") {
      val writer = Reader.writable()
      val client = connect(service(writer))
      val reader = Await.result(client(Request())).reader
      Await.result(writer.write(buf("hello")))
      assert(Await.result(readNBytes(5, reader)) == Buf.Utf8("hello"))
      Await.result(writer.write(buf("world")))
      assert(Await.result(readNBytes(5, reader)) == Buf.Utf8("world"))
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
      Await.result(client(req))
      client.close()
      intercept[ChannelClosedException] { Await.result(p) }
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

      Await.result(req.writer.write(buf("hello")))

      val contentf = resf flatMap { res => Reader.readAll(res.reader) }
      assert(Await.result(contentf) == Buf.Utf8("hello"))

      // drip should terminate because the request is discarded.
      intercept[Reader.ReaderDiscarded] { Await.result(drip(req.writer)) }
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
      Await.result(client(Request()))
      Await.result(client(Request()))
      client.close()
    }
  }

  def tracing(name: String)(connect: HttpService => HttpService) {
    test(name + ": trace") {
      var (outerTrace, outerSpan) = ("", "")

      val inner = connect(new HttpService {
        def apply(request: Request) = {
          val response = Response(request)
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

      val response = Await.result(outer(Request()))
      val Seq(innerTrace, innerSpan, innerParent) =
        response.contentString.split('.').toSeq
      assert(innerTrace == outerTrace, "traceId")
      assert(outerSpan == innerParent, "outer span vs inner parent")
      assert(innerSpan != outerSpan, "inner (%s) vs outer (%s) spanId".format(innerSpan, outerSpan))

      outer.close()
      inner.close()
    }
  }

  run("ClientBuilder")(standardErrors, standardBehaviour) {
    service =>
      val server = ServerBuilder()
        .codec(Http().maxRequestSize(100.bytes))
        .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
        .name("server")
        .build(service)

      val client = ClientBuilder()
        .codec(Http())
        .hosts(Seq(server.boundAddress))
        .hostConnectionLimit(1)
        .name("client")
        .build()

      new ServiceProxy(client) {
        override def close(deadline: Time) =
          Closable.all(client, server).close(deadline)
      }
  }

  run("Client/Server")(standardErrors, standardBehaviour, tracing) {
    service =>
      import com.twitter.finagle
      val server = finagle.Http.server.withMaxRequestSize(100.bytes).serve("localhost:*", service)
      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
      val client = finagle.Http.newService("%s:%d".format(addr.getHostName, addr.getPort))

      new ServiceProxy(client) {
        override def close(deadline: Time) =
          Closable.all(client, server).close(deadline)
      }
  }

  run("ClientBuilder (streaming)")(streaming) {
    service =>
      val server = ServerBuilder()
        .codec(Http().streaming(true))
        .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
        .name("server")
        .build(service)

      val client = ClientBuilder()
        .codec(Http().streaming(true))
        .hosts(Seq(server.boundAddress))
        .hostConnectionLimit(1)
        .name("client")
        .build()

      new ServiceProxy(client) {
        override def close(deadline: Time) =
          Closable.all(client, server).close(deadline)
      }
  }

  run("ClientBuilder (tracing)")(tracing) {
    service =>
      val server = ServerBuilder()
        .codec(Http().enableTracing(true))
        .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
        .name("server")
        .build(service)

      val client = ClientBuilder()
        .codec(Http().enableTracing(true))
        .hosts(Seq(server.boundAddress))
        .hostConnectionLimit(1)
        .name("client")
        .build()

      new ServiceProxy(client) {
        override def close(deadline: Time) =
          Closable.all(client, server).close(deadline)
      }
  }

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
      intercept[Exception](Await.result(client(Request())))

      assert(st.counters(Seq(clientName, "failure_accrual", "removals")) == 1)
      assert(st.counters(Seq(clientName, "retries", "requeues")) == failureAccrualFailures - 1)
      assert(st.counters(Seq(clientName, "failures", "restartable")) == failureAccrualFailures)
      client.close()
    }
  }

  status("ClientBuilder") {
    (service, st, name) =>
      val server = ServerBuilder()
        .codec(Http())
        .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
        .name("server")
        .build(service)

      val client = ClientBuilder()
        .codec(Http())
        .hosts(Seq(server.boundAddress))
        .hostConnectionLimit(1)
        .name(name)
        .failureAccrualParams((failureAccrualFailures, 1.minute))
        .reportTo(st)
        .build()

      new ServiceProxy(client) {
        override def close(deadline: Time) =
          Closable.all(client, server).close(deadline)
      }
  }

  status("Client/Server") {
    (service, st, name) =>
      import com.twitter.finagle
      val server = finagle.Http.serve(new InetSocketAddress(0), service)
      val client = finagle.Http.client
        .configured(Stats(st))
        .configured(FailureAccrualFactory.Param(failureAccrualFailures, () => 1.minute))
        .newService(Name.bound(server.boundAddress), name)

      new ServiceProxy(client) {
        override def close(deadline: Time) =
          Closable.all(client, server).close(deadline)
      }
  }

  test("codec should require a message size be less than 2Gb") {
    intercept[IllegalArgumentException](Http().maxRequestSize(2.gigabytes))
    intercept[IllegalArgumentException](Http(_maxResponseSize = 100.gigabytes))
    intercept[IllegalArgumentException] {
      com.twitter.finagle.Http.server.withMaxRequestSize(2049.megabytes)
    }
    intercept[IllegalArgumentException] {
      com.twitter.finagle.Http.client.withMaxResponseSize(3000.megabytes)
    }
  }
}
