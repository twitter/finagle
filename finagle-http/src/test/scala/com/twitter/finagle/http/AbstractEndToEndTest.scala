package com.twitter.finagle.http

import com.google.common.util.concurrent.AtomicDouble
import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.finagle
import com.twitter.finagle._
import com.twitter.finagle.context.{Contexts, Deadline, Retries}
import com.twitter.finagle.filter.MonitorFilter
import com.twitter.finagle.http.service.HttpResponseClassifier
import com.twitter.finagle.service.{
  ConstantService,
  FailureAccrualFactory,
  ResponseClass,
  ServiceFactoryRef
}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver, ReadableCounter}
import com.twitter.finagle.toggle.flag
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.util.HashedWheelTimer
import com.twitter.io.{Buf, Reader, Writer}
import com.twitter.util._
import java.io.{PrintWriter, StringWriter}
import java.net.InetSocketAddress
import org.scalatest.{BeforeAndAfter, FunSuite, OneInstancePerTest}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import scala.language.reflectiveCalls

abstract class AbstractEndToEndTest extends FunSuite
  with BeforeAndAfter
  with Eventually
  with IntegrationPatience
  with OneInstancePerTest {

  sealed trait Feature
  object MaxHeaderSize extends Feature
  object TooLongStream extends Feature

  var saveBase: Dtab = Dtab.empty
  val statsRecv: InMemoryStatsReceiver = new InMemoryStatsReceiver()

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
  type HttpTest = (HttpService => HttpService) => Unit

  def await[T](f: Future[T]): T = Await.result(f, 30.seconds)

  def drip(w: Writer): Future[Unit] = w.write(buf("*")) before drip(w)
  def buf(msg: String): Buf = Buf.Utf8(msg)
  def implName: String
  def clientImpl(): finagle.Http.Client
  def serverImpl(): finagle.Http.Server
  def initClient(client: HttpService): Unit = {}
  def initService: HttpService = Service.mk { req: Request =>
    Future.exception(new Exception("boom!"))
  }
  def featureImplemented(feature: Feature): Boolean
  def testIfImplemented(feature: Feature)(name: String)(testFn: => Unit): Unit = {
    if (!featureImplemented(feature)) ignore(name)(testFn) else test(name)(testFn)
  }

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
    Request("/", ("statusCode", status.code.toString))

  private val statusCodeSvc = new HttpService {
    def apply(request: Request) = {
      val statusCode = request.getIntParam("statusCode", Status.BadRequest.code)
      Future.value(Response(Status.fromCode(statusCode)))
    }
  }

  /**
   * Run the tests using the supplied connection generation function
   */
  def run(tests: HttpTest*)(connect: HttpService => HttpService): Unit = {
    tests.foreach(t => t(connect))
  }

  /**
   * Create a new non-streaming HTTP client/server pair and attach the service to the client
   */
  def nonStreamingConnect(service: HttpService): HttpService = {
    val ref = new ServiceFactoryRef(ServiceFactory.const(initService))
    val server = serverImpl()
      .withLabel("server")
      .withStatsReceiver(statsRecv)
      .withMaxRequestSize(100.bytes)
      .serve("localhost:*", ref)
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .withStatsReceiver(statsRecv)
      .newService("%s:%d".format(addr.getHostName, addr.getPort), "client")

    val ret = new ServiceProxy(client) {
      override def close(deadline: Time) =
        Closable.all(client, server).close(deadline)
    }
    initClient(client)
    ref() = ServiceFactory.const(service)
    ret
  }

  /**
   * Create a new streaming HTTP client/server pair and attach the service to the client
   */
  def streamingConnect(service: HttpService): HttpService = {
    val ref = new ServiceFactoryRef(ServiceFactory.const(initService))
    val server = serverImpl()
      .withStreaming(true)
      .withLabel("server")
      .withStatsReceiver(statsRecv)
      .serve("localhost:*", ref)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .withStreaming(true)
      .withStatsReceiver(statsRecv)
      .newService("%s:%d".format(addr.getHostName, addr.getPort), "client")

    initClient(client)
    ref() = ServiceFactory.const(service)
    new ServiceProxy(client) {
      override def close(deadline: Time) =
        Closable.all(client, server).close(deadline)
    }
  }

  def standardErrors(connect: HttpService => HttpService): Unit = {
    test(implName + ": request header fields too large") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response())
      }
      val client = connect(service)
      val request = Request("/")
      request.headers().add("header", "a" * 8192)
      val response = await(client(request))
      assert(response.status == Status.RequestHeaderFieldsTooLarge)
      await(client.close())
    }

    test(implName + ": with default client-side ResponseClassifier") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response())
      }
      val client = connect(service)

      await(client(requestWith(Status.Ok)))
      assert(statsRecv.counters(Seq("client", "requests")) == 1)
      assert(statsRecv.counters(Seq("client", "success")) == 1)

      await(client(requestWith(Status.ServiceUnavailable)))
      assert(statsRecv.counters(Seq("client", "requests")) == 2)
      // by default any `Return` is a successful response.
      assert(statsRecv.counters(Seq("client", "success")) == 2)

      await(client.close())
    }

    test(implName + ": with default server-side ResponseClassifier") {
      val client = connect(statusCodeSvc)

      await(client(requestWith(Status.Ok)))
      assert(statsRecv.counters(Seq("server", "requests")) == 1)
      assert(statsRecv.counters(Seq("server", "success")) == 1)

      await(client(requestWith(Status.ServiceUnavailable)))
      assert(statsRecv.counters(Seq("server", "requests")) == 2)
      // by default any `Return` is a successful response.
      assert(statsRecv.counters(Seq("server", "success")) == 2)

      await(client.close())
    }

    test(implName + ": unhandled exceptions are converted into 500s") {
      val service = new HttpService {
        def apply(request: Request) = Future.exception(new IllegalArgumentException("bad news"))
      }

      val client = connect(service)
      val response = await(client(Request("/")))
      assert(response.status == Status.InternalServerError)
      await(client.close())
    }

    test(implName + ": return 413s for fixed-length requests with too large payloads") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response())
      }
      val client = connect(service)

      val tooBig = Request("/")
      tooBig.content = Buf.ByteArray.Owned(new Array[Byte](200))

      val justRight = Request("/")
      justRight.content = Buf.ByteArray.Owned(Array[Byte](100))

      assert(await(client(tooBig)).status == Status.RequestEntityTooLarge)
      assert(await(client(justRight)).status == Status.Ok)
      await(client.close())
    }

    testIfImplemented(TooLongStream)(implName +
      ": return 413s for chunked requests which stream too much data") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response())
      }
      val client = connect(service)

      val justRight = Request("/")
      assert(await(client(justRight)).status == Status.Ok)

      val tooMuch = Request("/")
      tooMuch.setChunked(true)
      val w = tooMuch.writer
      w.write(buf("a"*1000)).before(w.close)
      val res = await(client(tooMuch))
      assert(res.status == Status.RequestEntityTooLarge)
      await(client.close())
    }
  }

  def standardBehaviour(connect: HttpService => HttpService) {

    testIfImplemented(MaxHeaderSize)(implName + ": client stack observes max header size") {
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
      assert(!await(hasBar))
      await(client.close())
    }

    test(implName + ": client sets content length") {
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
      assert(await(client(req)).contentString == body.length.toString)
      await(client.close())
    }

    test(implName + ": echo") {
      val service = new HttpService {
        def apply(request: Request) = {
          val response = Response()
          response.contentString = request.uri
          Future.value(response)
        }
      }

      val client = connect(service)
      val response = client(Request("123"))
      assert(await(response).contentString == "123")
      await(client.close())
    }

    test(implName + ": dtab") {
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

        val res = await(client(Request("/")))
        assert(res.contentString == "Dtab(2)\n\t/a => /b\n\t/c => /d\n")
      }


      await(client.close())
    }

    test(implName + ": (no) dtab") {
      val service = new HttpService {
        def apply(request: Request) = {
          val stringer = new StringWriter

          val response = Response(request)
          response.contentString = "%d".format(Dtab.local.length)
          Future.value(response)
        }
      }

      val client = connect(service)

      val res = await(client(Request("/")))
      assert(res.contentString == "0")

      await(client.close())
    }

    test(implName + ": context") {
      val writtenDeadline = Deadline.ofTimeout(5.seconds)
      val service = new HttpService {
        def apply(request: Request) = {
          val deadline = Deadline.current.get
          assert(deadline.deadline == writtenDeadline.deadline)

          val retries = Retries.current.get
          assert(retries == Retries(0))

          val response = Response(request)
          Future.value(response)
        }
      }

      Contexts.broadcast.let(Deadline, writtenDeadline) {
        val req = Request()
        val client = connect(service)
        val res = await(client(Request("/")))
        assert(res.status == Status.Ok)
        await(client.close())
      }
    }

    test(implName + ": client abort") {
      import com.twitter.conversions.time._
      val timer = new JavaTimer
      val promise = new Promise[Response]
      val service = new HttpService {
        def apply(request: Request) = promise
      }
      val client = connect(service)
      client(Request())
      await(timer.doLater(20.milliseconds) {
        await(client.close())
        intercept[CancelledRequestException] {
          promise.isInterrupted match {
            case Some(intr) => throw intr
            case _ =>
          }
        }
      })
    }

    test(implName + ": measure payload size") {
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
      await(client(req))

      assert(statsRecv.stat("client", "request_payload_bytes")() == Seq(10.0f))
      assert(statsRecv.stat("client", "response_payload_bytes")() == Seq(20.0f))
      assert(statsRecv.stat("server", "request_payload_bytes")() == Seq(10.0f))
      assert(statsRecv.stat("server", "response_payload_bytes")() == Seq(20.0f))
      await(client.close())
    }
  }

  def streaming(connect: HttpService => HttpService) {
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

    test(s"$implName (streaming)" + ": stream") {
      val writer = Reader.writable()
      val client = connect(service(writer))
      val reader = await(client(Request())).reader
      await(writer.write(buf("hello")))
      assert(await(readNBytes(5, reader)) == Buf.Utf8("hello"))
      await(writer.write(buf("world")))
      assert(await(readNBytes(5, reader)) == Buf.Utf8("world"))
      await(client.close())
    }

    test(s"$implName (streaming)" + ": stream via ResponseProxy filter") {
      class ResponseProxyFilter extends SimpleFilter[Request, Response] {
        override def apply(
          request: Request,
          service: Service[Request, Response]
        ): Future[Response] = {
          service(request).map { responseOriginal =>
            new ResponseProxy {
              override val response = responseOriginal
              override def reader = responseOriginal.reader
            }
          }
        }
      }

      def service = new HttpService {
        def apply(request: Request) = {
          val response = Response()
          response.setChunked(true)
          response.writer.write(buf("goodbye")).before {
            response.writer.write(buf("world")).before {
              response.close()
            }
          }
          Future.value(response)
        }
      }

      val serviceWithResponseProxy = (new ResponseProxyFilter).andThen(service)

      val client = connect(serviceWithResponseProxy)
      val response = await(client(Request()))
      val Buf.Utf8(actual) = await(Reader.readAll(response.reader))
      assert(actual == "goodbyeworld")
      await(client.close())
    }

    test(s"$implName (streaming)" + ": stream via ResponseProxy class") {
      case class EnrichedResponse(resp: Response) extends ResponseProxy {
        override val response = resp
      }

      // Test streaming partial data separated in time
      def service = new HttpService {
        def apply(request: Request) = {
          val response = EnrichedResponse(Response(Version.Http11, Status.Ok))
          response.setChunked(true)

          response.writer.write(Buf.Utf8("hello")) before {
            Future.sleep(Duration.fromSeconds(3))(HashedWheelTimer.Default) before {
              response.writer.write(Buf.Utf8("world")) ensure {
                response.close()
              }
            }
          }

          Future.value(response)
        }
      }

      val client = connect(service)
      val response = await(client(Request()))
      val Buf.Utf8(actual) = await(Reader.readAll(response.reader))
      assert(actual == "helloworld")
      await(client.close())
    }

    test(s"$implName (streaming)" + ": streaming clients can decompress content") {
      val svc = new Service[Request, Response] {
        def apply(request: Request) = {
          val response = Response()
          response.contentString = "raw content"
          Future.value(response)
        }
      }
      val client = connect(svc)
      val req = Request("/")
      req.headerMap.set("accept-encoding", "gzip")

      val content = await(client(req).flatMap { rep => Reader.readAll(rep.reader) })
      assert(Buf.Utf8.unapply(content).get == "raw content")
      await(client.close())
    }

    test(s"$implName (streaming)" + ": symmetric reader and getContent") {
      val s = Service.mk[Request, Response] { req =>
        val buf = await(Reader.readAll(req.reader))
        assert(buf == Buf.Utf8("hello"))
        assert(req.contentString == "hello")

        req.response.content = req.content
        Future.value(req.response)
      }
      val req = Request()
      req.contentString = "hello"
      req.headerMap.put("Content-Length", "5")
      val client = connect(s)
      val res = await(client(req))

      val buf = await(Reader.readAll(res.reader))
      assert(buf == Buf.Utf8("hello"))
      assert(res.contentString == "hello")
    }

    test(s"$implName (streaming): transport closure propagates to request stream reader") {
      val p = new Promise[Buf]
      val s = Service.mk[Request, Response] { req =>
        p.become(Reader.readAll(req.reader))
        Future.value(Response())
      }
      val client = connect(s)
      val req = Request()
      req.setChunked(true)
      await(client(req))
      await(client.close())
      intercept[ChannelClosedException] { await(p) }
    }

    test(s"$implName (streaming)" +
      ": transport closure propagates to request stream producer") {
      val s = Service.mk[Request, Response] { _ => Future.value(Response()) }
      val client = connect(s)
      val req = Request()
      req.setChunked(true)
      client(req)
      await(client.close())
      intercept[Reader.ReaderDiscarded] { await(drip(req.writer)) }
    }

    test(s"$implName (streaming): " +
      "request discard terminates remote stream producer") {
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

      await(req.writer.write(buf("hello")))

      val contentf = resf flatMap { res => Reader.readAll(res.reader) }
      assert(await(contentf) == Buf.Utf8("hello"))

      // drip should terminate because the request is discarded.
      intercept[Reader.ReaderDiscarded] { await(drip(req.writer)) }
    }

    test(s"$implName (streaming): " +
      "client discard terminates stream and frees up the connection") {
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
      val rep = await(client(Request()))
      assert(s.rep != null)
      rep.reader.discard()

      s.rep = null

      // Now, make sure the connection doesn't clog up.
      await(client(Request()))
      assert(s.rep != null)
    }

    test(s"$implName (streaming)" + ": two fixed-length requests") {
      val svc = Service.mk[Request, Response] { _ => Future.value(Response()) }
      val client = connect(svc)
      await(client(Request()))
      await(client(Request()))
      await(client.close())
    }

    test(s"$implName (streaming)" +": does not measure payload size") {
      val svc = Service.mk[Request, Response] { _ => Future.value(Response()) }
      val client = connect(svc)
      await(client(Request()))

      assert(statsRecv.stat("client", "request_payload_bytes")() == Nil)
      assert(statsRecv.stat("client", "response_payload_bytes")() == Nil)
      assert(statsRecv.stat("server", "request_payload_bytes")() == Nil)
      assert(statsRecv.stat("server", "response_payload_bytes")() == Nil)
      await(client.close())
    }
  }

  def tracing(connect: HttpService => HttpService) {
    test(implName + ": trace") {
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

      val response = await(outer(Request()))
      val Seq(innerTrace, innerSpan, innerParent) =
        response.contentString.split('.').toSeq
      assert(innerTrace == outerTrace, "traceId")
      assert(outerSpan == innerParent, "outer span vs inner parent")
      assert(innerSpan != outerSpan, "inner (%s) vs outer (%s) spanId".format(innerSpan, outerSpan))

      await(outer.close())
      await(inner.close())
    }
  }

  run(standardErrors, standardBehaviour, tracing)(nonStreamingConnect(_))

  run(streaming)(streamingConnect(_))

  // use 1 less than the requeue limit so that we trigger failure accrual
  // before we run into the requeue limit.
  private val failureAccrualFailures = 19

  test(implName + ": Status.busy propagates along the Stack") {
    val st = new InMemoryStatsReceiver
    val failService = new HttpService {
      def apply(req: Request): Future[Response] =
        Future.exception(Failure.rejected("unhappy"))
    }

    val clientName = "http"
    val server = serverImpl().serve(new InetSocketAddress(0), failService)
    val client = clientImpl()
      .withStatsReceiver(st)
      .configured(FailureAccrualFactory.Param(failureAccrualFailures, () => 1.minute))
      .newService(
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        clientName
      )

    val e = intercept[Exception](await(client(Request("/"))))

    assert(st.counters(Seq(clientName, "failure_accrual", "removals")) == 1)
    assert(st.counters(Seq(clientName, "retries", "requeues")) == failureAccrualFailures - 1)
    assert(st.counters(Seq(clientName, "failures", "restartable")) == failureAccrualFailures)
    await(Closable.all(client, server).close())
  }

  test("Client-side ResponseClassifier based on status code") {
    val classifier = HttpResponseClassifier {
      case (_, r: Response) if r.status == Status.ServiceUnavailable =>
        ResponseClass.NonRetryableFailure
    }

    val server = serverImpl()
      .withStatsReceiver(NullStatsReceiver)
      .serve("localhost:*", statusCodeSvc)
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .withStatsReceiver(statsRecv)
      .withResponseClassifier(classifier)
      .newService("%s:%d".format(addr.getHostName, addr.getPort), "client")

    val rep1 = await(client(requestWith(Status.Ok)))
    assert(statsRecv.counters(Seq("client", "requests")) == 1)
    assert(statsRecv.counters(Seq("client", "success")) == 1)

    val rep2 = await(client(requestWith(Status.ServiceUnavailable)))

    assert(statsRecv.counters(Seq("client", "requests")) == 2)
    assert(statsRecv.counters(Seq("client", "success")) == 1)

    await(client.close())
    await(server.close())
  }

  test("server-side ResponseClassifier based on status code") {
    val classifier = HttpResponseClassifier {
      case (_, r: Response) if r.status == Status.ServiceUnavailable =>
        ResponseClass.NonRetryableFailure
    }

    val server = serverImpl()
      .withResponseClassifier(classifier)
      .withStatsReceiver(statsRecv)
      .withLabel("server")
      .serve("localhost:*", statusCodeSvc)
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .newService("%s:%d".format(addr.getHostName, addr.getPort), "client")

    await(client(requestWith(Status.Ok)))
    assert(statsRecv.counters(Seq("server", "requests")) == 1)
    assert(statsRecv.counters(Seq("server", "success")) == 1)

    await(client(requestWith(Status.ServiceUnavailable)))
    assert(statsRecv.counters(Seq("server", "requests")) == 2)
    assert(statsRecv.counters(Seq("server", "success")) == 1)
    assert(statsRecv.counters(Seq("server", "failures")) == 1)

    await(client.close())
    await(server.close())
  }

  test("codec should require a message size be less than 2Gb") {
    intercept[IllegalArgumentException](Http().maxRequestSize(2.gigabytes))
    intercept[IllegalArgumentException](Http(_maxResponseSize = 100.gigabytes))
    intercept[IllegalArgumentException] {
      serverImpl().withMaxRequestSize(2049.megabytes)
    }
    intercept[IllegalArgumentException] {
      clientImpl().withMaxResponseSize(3000.megabytes)
    }
  }

  test("non-streaming clients can decompress content") {
    val svc = new Service[Request, Response] {
      def apply(request: Request) = {
        val response = Response()
        response.contentString = "raw content"
        Future.value(response)
      }
    }
    val server = serverImpl()
      .withStatsReceiver(NullStatsReceiver)
      .withCompressionLevel(5)
      .serve("localhost:*", svc)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .withStatsReceiver(NullStatsReceiver)
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    val req = Request("/")
    req.headerMap.set("accept-encoding", "gzip")
    assert(await(client(req)).contentString == "raw content")
    await(client.close())
    await(server.close())
  }

  test("request remote address") {
    val svc = new Service[Request, Response] {
      def apply(request: Request) = {
        val response = Response()
        response.contentString = request.remoteAddress.toString
        Future.value(response)
      }
    }
    val server = serverImpl()
      .serve("localhost:*", svc)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    assert(await(client(Request("/"))).contentString.startsWith("/127.0.0.1"))
    await(client.close())
    await(server.close())
  }

  test(implName + ": ResponseClassifier respects toggle") {
    import com.twitter.finagle.{Http => ctfHttp}

    val serverFraction = new AtomicDouble(-1.0)
    val serverToggleFilter = new SimpleFilter[Request, Response] {
      def apply(request: Request, service: Service[Request, Response]): Future[Response] = {
        val frac = serverFraction.get()
        if (frac < 0.0) {
          service(request)
        } else {
          var rep: Future[Response] = Future.exception(new RuntimeException("not init"))
          flag.overrides.let(ctfHttp.ServerErrorsAsFailuresToggleId, frac) {
            rep = service(request)
          }
          rep
        }
      }
    }
    val module = new Stack.Module0[ServiceFactory[Request, Response]] {
      val role: Stack.Role = Stack.Role("server response classifier")
      val description: String = role.toString
      def make(next: ServiceFactory[Request, Response]): ServiceFactory[Request, Response] =
        serverToggleFilter.andThen(next)
    }

    val srvImpl = serverImpl()
      .withLabel("server")
      .withStatsReceiver(statsRecv)
    val svc500s = new ConstantService[Request, Response](
      Future.value(Response(Status.InternalServerError)))

    val server = srvImpl
      // we need to inject a filter that'll set the fraction properly
      // for the server's flag's Local value
      .withStack(srvImpl.stack.insertBefore(MonitorFilter.role, module))
      .serve("localhost:*", svc500s)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .withStatsReceiver(statsRecv)
      .newService("%s:%d".format(addr.getHostName, addr.getPort), "client")

    val clientSuccesses: ReadableCounter = statsRecv.counter("client", "success")
    val clientFailures: ReadableCounter = statsRecv.counter("client", "failures")
    val serverSuccesses: ReadableCounter = statsRecv.counter("server", "success")
    val serverFailures: ReadableCounter = statsRecv.counter("server", "failures")

    def issueRequest(fraction: Option[Double]) = {
      val res = fraction match {
        case None =>
          serverFraction.set(-1.0)
          client(Request("/"))
        case Some(f) =>
          serverFraction.set(f)
          var r: Future[Response] = Future.exception(new RuntimeException("never init"))
          flag.overrides.let(ctfHttp.ServerErrorsAsFailuresToggleId, f) {
            r = client(Request("/"))
          }
          r
      }
      assert(Status.InternalServerError == await(res).status)
    }

    // as tested above, the default is that 500s are successful
    issueRequest(None)
    eventually {
      assert(1 == clientSuccesses())
      assert(0 == clientFailures())
      assert(1 == serverSuccesses())
      assert(0 == serverFailures())
    }

    // switch to 500s as failures
    issueRequest(Some(1.0))
    eventually {
      assert(1 == clientSuccesses())
      assert(1 == clientFailures())
      assert(1 == serverSuccesses())
      assert(1 == serverFailures())
    }

    // switch it back to 500s are ok
    issueRequest(Some(0.0))
    eventually {
      assert(2 == clientSuccesses())
      assert(1 == clientFailures())
      assert(2 == serverSuccesses())
      assert(1 == serverFailures())
    }

    await(server.close())
    await(client.close())
  }
}
