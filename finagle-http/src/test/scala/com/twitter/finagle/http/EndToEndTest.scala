package com.twitter.finagle.http

import com.twitter.conversions.time._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.netty3.ChannelBufferBuf
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.{
  CancelledRequestException, ChannelClosedException, Dtab, Service, ServiceProxy}
import com.twitter.io.{Buf, Reader, Writer}
import com.twitter.util.{
  Await, Closable, Future, Promise, Time, JavaTimer, Return, Throw}
import java.io.{StringWriter, PrintWriter}
import java.net.{InetAddress, InetSocketAddress}
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._
import org.junit.runner.RunWith
import org.scalatest.concurrent.Eventually
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends FunSuite with BeforeAndAfter with Eventually {
  var saveBase: Dtab = Dtab.empty
  before {
    saveBase = Dtab.base
    Dtab.base = Dtab.read("/foo=>/bar; /baz=>/biz")
  }

  after {
    Dtab.base = saveBase
  }

  type HttpService = Service[HttpRequest, HttpResponse]
  type RichHttpService = Service[Request, Response]

  def drip(w: Writer): Future[Unit] = w.write(buf("*")) before drip(w)
  def buf(msg: String): Buf =
    ChannelBufferBuf.Owned(
      ChannelBuffers.wrappedBuffer(msg.getBytes("UTF-8")))

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

  def go(name: String)(connect: HttpService => HttpService) {
    test(name + ": client stack observes max header size") {
      import scala.collection.JavaConverters._
      val service = new HttpService {
        def apply(req: HttpRequest) = {
          val res = Response()
          res.headers.set("Foo", ("*" * 8192) + "Bar: a")
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
          val names = res.headers.names().asScala
          Future.value(names.exists(_.contains("Bar")))
      }

      assert(!Await.result(hasBar))
      client.close()
    }

    test(name + ": echo") {
      val service = new HttpService {
        def apply(request: HttpRequest) = {
          val response = Response(request)
          response.contentString = Request(request).uri
          Future.value(response)
        }
      }

      val client = connect(service)
      val response = client(Request("123"))
      assert(Response(Await.result(response)).contentString === "123")
      client.close()
    }

    test(name + ": dtab") {
      val service = new HttpService {
        def apply(request: HttpRequest) = {
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

        val res = Response(Await.result(client(Request("/"))))
        assert(res.contentString === "Dtab(2)\n\t/a => /b\n\t/c => /d\n")
      }

      client.close()
    }

    test(name + ": (no) dtab") {
      val service = new HttpService {
        def apply(request: HttpRequest) = {
          val stringer = new StringWriter
          val printer = new PrintWriter(stringer)

          val response = Response(request)
          response.contentString = "%d".format(Dtab.local.length)
          Future.value(response)
        }
      }

      val client = connect(service)

      val res = Response(Await.result(client(Request("/"))))
      assert(res.contentString === "0")

      client.close()
    }

    test(name + ": stream") {
      def service(r: Reader) = new HttpService {
        def apply(request: HttpRequest) = {
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
      val response = Response(Await.result(client(Request())))
      assert(response.contentString === "helloworld")
      client.close()
    }

    test(name + ": client abort") {
      import com.twitter.conversions.time._
      val timer = new JavaTimer
      val promise = new Promise[Response]
      val service = new HttpService {
        def apply(request: HttpRequest) = promise
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

    test(name + ": request uri too long") {
      val ok = Response()
      val service = new HttpService {
        def apply(request: HttpRequest) = Future.value(ok)
      }
      val client = connect(service)
      val request = Request("/" + "a" * 4096)
      val response = Await.result(client(request))
      assert(response.getStatus === HttpResponseStatus.REQUEST_URI_TOO_LONG)

      // Subsequent valid requests should succeed.
      assert(Await.result(client(Request())).getStatus === ok.status)

      client.close()
    }

    test(name + ": request header fields too large") {
      val service = new HttpService {
        def apply(request: HttpRequest) = Future.value(Response())
      }
      val client = connect(service)
      val request = Request()
      request.headers().add("header", "a" * 8192)
      val response = Await.result(client(request))
      assert(response.getStatus === HttpResponseStatus.REQUEST_HEADER_FIELDS_TOO_LARGE)
      client.close()
    }
  }

  def rich(name: String)(connect: RichHttpService => RichHttpService) {
    def service(r: Reader) = new RichHttpService {
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
        assert(buf === Buf.Utf8("hello"))
        assert(req.contentString === "hello")

        req.response.setContent(req.getContent)
        Future.value(req.response)
      }
      val req = Request()
      req.contentString = "hello"
      req.headers.set("Content-Length", 5)
      val client = connect(s)
      val res = Await.result(client(req))

      val buf = Await.result(Reader.readAll(res.reader))
      assert(buf === Buf.Utf8("hello"))
      assert(res.contentString === "hello")
    }

    test(name + ": stream") {
      val writer = Reader.writable()
      val client = connect(service(writer))
      val reader = Await.result(client(Request())).reader
      Await.result(writer.write(buf("hello")))
      assert(Await.result(readNBytes(5, reader)) === Buf.Utf8("hello"))
      Await.result(writer.write(buf("world")))
      assert(Await.result(readNBytes(5, reader)) === Buf.Utf8("world"))
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
      intercept[Reader.ReaderDiscarded] {
        Await.result(
          client.close() before
          drip(req.writer), 5.seconds)
      }
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
      assert(Await.result(contentf) === Buf.Utf8("hello"))

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

    test(name + ": request uri too long") {
      val svc = Service.mk[Request, Response] { _ => Future.value(Response()) }
      val client = connect(svc)
      val request = Request("/" + "a" * 4096)
      val response = Await.result(client(request))
      assert(response.getStatus === HttpResponseStatus.REQUEST_URI_TOO_LONG)
      client.close()
    }

    test(name + ": request header fields too large") {
      val svc = Service.mk[Request, Response] { _ => Future.value(Response()) }
      val client = connect(svc)
      val request = Request()
      request.headers.add("header", "a" * 8192)
      val response = Await.result(client(request))
      assert(response.getStatus === HttpResponseStatus.REQUEST_HEADER_FIELDS_TOO_LARGE)
      client.close()
    }
  }

  def trace(name: String)(connect: HttpService => HttpService) {
    test(name + ": trace") {
      var (outerTrace, outerSpan) = ("", "")

      val inner = connect(new HttpService {
        def apply(request: HttpRequest) = {
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
        def apply(request: HttpRequest) = {
          outerTrace = Trace.id.traceId.toString
          outerSpan = Trace.id.spanId.toString
          inner(request)
        }
      })

      val response = Response(Await.result(outer(Request())))
      val Seq(innerTrace, innerSpan, innerParent) =
        response.contentString.split('.').toSeq
      assert(innerTrace === outerTrace, "traceId")
      assert(outerSpan === innerParent, "outer span vs inner parent")
      assert(innerSpan != outerSpan, "inner (%s) vs outer (%s) spanId".format(innerSpan, outerSpan))

      outer.close()
      inner.close()
    }
  }

  trace("Client/Server") {
    service =>
      import com.twitter.finagle.Http
      val server = Http.serve("localhost:*", service)
      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
      val client = Http.newService("%s:%d".format(addr.getHostName, addr.getPort))

      new ServiceProxy(client) {
        override def close(deadline: Time) =
          Closable.all(client, server).close(deadline)
      }
  }

  trace("ClientBuilder") {
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

  go("ClientBuilder") {
    service =>
      val server = ServerBuilder()
        .codec(Http())
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

  go("Client/Server") {
    service =>
      import com.twitter.finagle.Http
      val server = Http.serve("localhost:*", service)
      val client = Http.newService(server)

      new ServiceProxy(client) {
        override def close(deadline: Time) =
          Closable.all(client, server).close(deadline)
      }
  }

  rich("ClientBuilder (RichHttp)") {
    service =>
      val server = ServerBuilder()
        .codec(RichHttp[Request](Http(), aggregateChunks = false))
        .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
        .name("server")
        .build(service)

      val client = ClientBuilder()
        .codec(RichHttp[Request](Http(), aggregateChunks = false))
        .hosts(Seq(server.boundAddress))
        .hostConnectionLimit(1)
        .name("client")
        .build()

      new ServiceProxy(client) {
        override def close(deadline: Time) =
          Closable.all(client, server).close(deadline)
      }
  }

}
