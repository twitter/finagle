package com.twitter.finagle.http

import com.twitter.conversions.storage._
import com.twitter.conversions.time._
import com.twitter.finagle
import com.twitter.finagle._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.context.{Contexts, Deadline, Retries}
import com.twitter.finagle.filter.ServerAdmissionControl
import com.twitter.finagle.http.service.HttpResponseClassifier
import com.twitter.finagle.http2.param.EncoderIgnoreMaxHeaderListSize
import com.twitter.finagle.liveness.FailureAccrualFactory
import com.twitter.finagle.service._
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.util.HashedWheelTimer
import com.twitter.io.{Buf, Reader, Writer}
import com.twitter.util._
import io.netty.buffer.PooledByteBufAllocator
import java.io.{PrintWriter, StringWriter}
import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.atomic.AtomicBoolean
import org.scalactic.source.Position
import org.scalatest.{BeforeAndAfter, FunSuite, OneInstancePerTest, Tag}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import scala.language.reflectiveCalls

abstract class AbstractEndToEndTest
    extends FunSuite
    with BeforeAndAfter
    with Eventually
    with IntegrationPatience
    with OneInstancePerTest {

  sealed trait Feature
  object TooLongStream extends Feature
  object ClientAbort extends Feature
  object HeaderFields extends Feature
  object ReaderClose extends Feature
  object NoBodyMessage extends Feature
  object AutomaticContinue extends Feature
  object DisableAutomaticContinue extends Feature
  object SetsPooledAllocatorMaxOrder extends Feature

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
  def skipWholeTest: Boolean = false
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
    def apply(request: Request): Future[Response] = {
      val statusCode = request.getIntParam("statusCode", Status.BadRequest.code)
      Future.value(Response(Status.fromCode(statusCode)))
    }
  }

  override def test(testName: String, testTags: Tag*)(
    testFun: => Any
  )(implicit pos: Position): Unit = {
    if (skipWholeTest)
      ignore(testName)(testFun)
    else
      super.test(testName, testTags: _*)(testFun)
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
      .withMaxHeaderSize(8.kilobytes)
      .withMaxRequestSize(200.bytes)
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
    testIfImplemented(HeaderFields)(implName + ": request header fields too large") {
      val service = new HttpService {
        def apply(request: Request) = Future.value(Response())
      }
      val client = connect(service)
      val request = Request("/")
      request.headerMap.add("header", "a" * 8192)
      val response = await(client(request))
      assert(response.status == Status.RequestHeaderFieldsTooLarge)
      await(client.close())
    }

    test(implName + ": with default client-side ResponseClassifier") {
      val client = connect(statusCodeSvc)

      await(client(requestWith(Status.Ok)))
      assert(statsRecv.counters(Seq("client", "requests")) == 1)
      assert(statsRecv.counters(Seq("client", "success")) == 1)

      await(client(requestWith(Status.ServiceUnavailable)))
      assert(statsRecv.counters(Seq("client", "requests")) == 2)
      // by default 500s are treated as unsuccessful
      assert(statsRecv.counters(Seq("client", "success")) == 1)

      await(client.close())
    }

    test(implName + ": with default server-side ResponseClassifier") {
      val client = connect(statusCodeSvc)

      await(client(requestWith(Status.Ok)))
      assert(statsRecv.counters(Seq("server", "requests")) == 1)
      assert(statsRecv.counters(Seq("server", "success")) == 1)

      await(client(requestWith(Status.ServiceUnavailable)))
      assert(statsRecv.counters(Seq("server", "requests")) == 2)
      // by default 500s are treated as unsuccessful
      assert(statsRecv.counters(Seq("server", "success")) == 1)

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
      tooBig.content = Buf.ByteArray.Owned(new Array[Byte](300))

      val justRight = Request("/")
      justRight.content = Buf.ByteArray.Owned(new Array[Byte](200))

      assert(await(client(tooBig)).status == Status.RequestEntityTooLarge)
      assert(await(client(justRight)).status == Status.Ok)
      await(client.close())
    }

    if (!sys.props.contains("SKIP_FLAKY"))
      testIfImplemented(TooLongStream)(
        implName +
          ": return 413s for chunked requests which stream too much data"
      ) {
        val service = new HttpService {
          def apply(request: Request) = Future.value(Response())
        }
        val client = connect(service)

        val justRight = Request("/")
        assert(await(client(justRight)).status == Status.Ok)

        val tooMuch = Request("/")
        tooMuch.setChunked(true)
        val w = tooMuch.writer
        w.write(buf("a" * 1000)).before(w.close)
        val res = await(client(tooMuch))
        assert(res.status == Status.RequestEntityTooLarge)
        await(client.close())
      }
  }

  def standardBehaviour(connect: HttpService => HttpService) {

    test(implName + ": client stack observes max header size") {
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

    if (!sys.props.contains("SKIP_FLAKY"))
      testIfImplemented(ClientAbort)(implName + ": client abort") {
        import com.twitter.conversions.time._
        val timer = new JavaTimer
        val promise = new Promise[Response]
        val service = new HttpService {
          def apply(request: Request) = promise
        }
        val client = connect(service)
        client(Request())
        await(timer.doLater(20.milliseconds) {
          await(client.close(20.milliseconds))
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

      eventually {
        assert(statsRecv.stat("client", "request_payload_bytes")() == Seq(10.0f))
        assert(statsRecv.stat("client", "response_payload_bytes")() == Seq(20.0f))
        assert(statsRecv.stat("server", "request_payload_bytes")() == Seq(10.0f))
        assert(statsRecv.stat("server", "response_payload_bytes")() == Seq(20.0f))
      }

      await(client.close())
    }

    test(implName + ": interrupt requests") {
      val p = Promise[Unit]()
      val interrupted = Promise[Unit]()

      val service = new HttpService {
        def apply(request: Request) = {
          p.setDone()
          val interruptee = Promise[Response]()
          interruptee.setInterruptHandler {
            case exn: Throwable =>
              interrupted.setDone()
          }
          interruptee
        }
      }

      val client = connect(service)
      val req = Request()
      req.content = Buf.Utf8("." * 10)
      val f = client(req)
      await(p)
      assert(!f.isDefined)

      val e = new Exception("boom!")
      f.raise(e)
      val actual = intercept[Exception] {
        await(f)
      }
      assert(actual == e)
      await(interrupted)

      await(client.close())
    }

    test(implName + ": interrupting requests doesn't interfere with others") {
      val p = Promise[Unit]()
      val interrupted = Promise[Unit]()

      val second = Promise[Response]
      val service = new HttpService {
        def apply(request: Request) = {
          if (!p.isDefined) {
            p.setDone()
            val interruptee = Promise[Response]()
            interruptee.setInterruptHandler {
              case exn: Throwable =>
                interrupted.setDone()
            }
            interruptee
          } else {
            second
          }
        }
      }

      val client = connect(service)
      val req = Request()
      req.content = Buf.Utf8("." * 10)
      val f1 = client(req)
      await(p)
      val f2 = client(req)
      assert(!f1.isDefined)
      assert(!f2.isDefined)

      val e = new Exception("boom!")
      f1.raise(e)
      val actual = intercept[Exception] {
        await(f1)
      }
      assert(actual == e)
      await(interrupted)

      assert(!f2.isDefined)

      second.setValue(req.response)
      assert(await(f2).status == Status.Ok)

      await(client.close())
    }
  }

  def streaming(connect: HttpService => HttpService) {
    test(s"$implName (streaming)" + ": stream") {
      def service(r: Reader) = new HttpService {
        def apply(request: Request) = {
          val response = Response.chunked(Version.Http11, Status.Ok, r)
          Future.value(response)
        }
      }

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

    test(s"$implName (streaming): aggregates responses that must not have a body") {
      val service = new HttpService {
        def apply(request: Request): Future[Response] = {
          val resp = Response()
          resp.status = Status.NoContent
          Future.value(resp)
        }
      }

      val client = connect(service)
      val resp = await(client(Request()))
      assert(!resp.isChunked)
      assert(resp.content.isEmpty)
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

      val content = await(client(req).flatMap { rep =>
        Reader.readAll(rep.reader)
      })
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

    testIfImplemented(ReaderClose)(
      s"$implName (streaming): transport closure propagates to request stream reader"
    ) {
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

    test(
      s"$implName (streaming)" +
        ": transport closure propagates to request stream producer"
    ) {
      val s = Service.mk[Request, Response] { _ =>
        Future.value(Response())
      }
      val client = connect(s)
      val req = Request()
      req.setChunked(true)
      await(client(req))
      await(client.close())
      intercept[Reader.ReaderDiscarded] { await(drip(req.writer)) }
    }

    test(
      s"$implName (streaming): " +
        "request discard terminates remote stream producer"
    ) {
      val s = Service.mk[Request, Response] { req =>
        val res = Response()
        res.setChunked(true)
        def go =
          for {
            Some(c) <- req.reader.read(Int.MaxValue)
            _ <- res.writer.write(c)
            _ <- res.close()
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

      val contentf = resf flatMap { res =>
        Reader.readAll(res.reader)
      }
      assert(await(contentf) == Buf.Utf8("hello"))

      // drip should terminate because the request is discarded.
      intercept[Reader.ReaderDiscarded] { await(drip(req.writer)) }
    }

    test(
      s"$implName (streaming): " +
        "client discard terminates stream and frees up the connection"
    ) {
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
      val svc = Service.mk[Request, Response] { _ =>
        Future.value(Response())
      }
      val client = connect(svc)
      await(client(Request()))
      await(client(Request()))
      await(client.close())
    }

    test(s"$implName (streaming)" + ": does not measure payload size") {
      val svc = Service.mk[Request, Response] { _ =>
        Future.value(Response())
      }
      val client = connect(svc)
      await(client(Request()))

      assert(statsRecv.stat("client", "request_payload_bytes")() == Nil)
      assert(statsRecv.stat("client", "response_payload_bytes")() == Nil)
      assert(statsRecv.stat("server", "request_payload_bytes")() == Nil)
      assert(statsRecv.stat("server", "response_payload_bytes")() == Nil)
      await(client.close())
    }

    def makeService(size: Int): Service[Request, Response] = {
      Service.mk[Request, Response] { req =>
        val resp = Response()
        resp.contentString = "*" * size
        resp.contentLength = size
        Future.value(resp)
      }
    }

    test("Responses with Content-length header larger than 8 KB are not aggregated") {
      val svc = makeService(8 * 1024 + 1)
      val client = connect(svc)
      val resp = await(client(Request()))
      assert(resp.isChunked)
      assert(resp.content.isEmpty)
      assert(resp.contentLength == Some(8 * 1024 + 1))
    }

    test("Responses with Content-length header equal to 8 KB are aggregated") {
      val svc = makeService(8 * 1024)
      val client = connect(svc)
      val resp = await(client(Request()))
      assert(!resp.isChunked)
      assert(!resp.content.isEmpty)
      assert(resp.contentLength == Some(8 * 1024))
    }

    test("Responses with Content-length header smaller than 8 KB are aggregated") {
      val svc = makeService(8 * 1024 - 1)
      val client = connect(svc)
      val resp = await(client(Request()))
      assert(!resp.isChunked)
      assert(!resp.content.isEmpty)
      assert(resp.contentLength == Some(8 * 1024 - 1))
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

  // use 1 less than the requeue limit so that we trigger failure accrual
  // before we run into the requeue limit.
  private val failureAccrualFailures = 19

  run(standardErrors, standardBehaviour, tracing)(nonStreamingConnect(_))

  run(streaming)(streamingConnect(_))

  testIfImplemented(SetsPooledAllocatorMaxOrder)(
    implName + ": PooledByteBufAllocator maxOrder " +
      "is 7 for servers"
  ) {
    // this will set the default order
    val server = serverImpl().serve(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new ConstantService[Request, Response](Future.value(Response()))
    )
    assert(PooledByteBufAllocator.defaultMaxOrder == 7)
    await(server.close())
  }

  testIfImplemented(SetsPooledAllocatorMaxOrder)(
    implName + ": PooledByteBufAllocator maxOrder " +
      "is 7 for clients"
  ) {
    val server = serverImpl().serve(
      new InetSocketAddress(InetAddress.getLoopbackAddress, 0),
      new ConstantService[Request, Response](Future.value(Response()))
    )

    // this will set the default order
    val client = clientImpl().newService(
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )
    client.apply(Request())
    assert(PooledByteBufAllocator.defaultMaxOrder == 7)
    await(Closable.all(client, server).close())
  }

  test(implName + ": Status.busy propagates along the Stack") {
    val failService = new HttpService {
      def apply(req: Request): Future[Response] =
        Future.exception(Failure.rejected("unhappy"))
    }

    val clientName = "http"
    val server = serverImpl().serve(new InetSocketAddress(0), failService)
    val client = clientImpl()
      .configured(FailureAccrualFactory.Param(failureAccrualFailures, () => 1.minute))
      .withStatsReceiver(statsRecv)
      .newService(
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        clientName
      )

    val e = intercept[FailureFlags[_]] {
      await(client(Request("/")))
    }
    assert(e.isFlagged(FailureFlags.Rejected))

    assert(statsRecv.counters(Seq(clientName, "failure_accrual", "removals")) == 1)
    assert(statsRecv.counters(Seq(clientName, "retries", "requeues")) == failureAccrualFailures - 1)
    assert(statsRecv.counters(Seq(clientName, "failures", "restartable")) == failureAccrualFailures)
    await(Closable.all(client, server).close())
  }

  test(implName + ": nonretryable isn't retried") {
    val failService = new HttpService {
      def apply(req: Request): Future[Response] =
        Future.exception(Failure("unhappy", FailureFlags.NonRetryable | FailureFlags.Rejected))
    }

    val clientName = "http"
    val server = serverImpl().serve(new InetSocketAddress(0), failService)
    val client = clientImpl()
      .configured(FailureAccrualFactory.Param(failureAccrualFailures, () => 1.minute))
      .withStatsReceiver(statsRecv)
      .newService(
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        clientName
      )

    val e = intercept[FailureFlags[_]] {
      val req = Request("/")
      await(client(req))
    }
    assert(e.isFlagged(FailureFlags.Rejected))

    assert(!statsRecv.counters.contains(Seq(clientName, "failure_accrual", "removals")))
    assert(!statsRecv.counters.contains(Seq(clientName, "retries", "requeues")))
    assert(!statsRecv.counters.contains(Seq(clientName, "failures", "restartable")))
    assert(statsRecv.counters(Seq(clientName, "failures")) == 1)
    assert(statsRecv.counters(Seq(clientName, "requests")) == 1)
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
    intercept[IllegalArgumentException] {
      serverImpl().withMaxRequestSize(2049.megabytes)
    }
    intercept[IllegalArgumentException] {
      clientImpl().withMaxResponseSize(3000.megabytes)
    }
  }

  test("client respects MaxResponseSize") {
    val svc = new HttpService {
      def apply(request: Request): Future[Response] = {
        val response = Response()
        response.contentString = "*" * 600.kilobytes.bytes.toInt
        Future.value(response)
      }
    }

    val server = serverImpl()
      .withStatsReceiver(NullStatsReceiver)
      .serve("localhost:*", svc)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .withMaxResponseSize(500.kilobytes) // wontfix: doesn't work on netty3 with limit <= 8 KB
      .withStatsReceiver(NullStatsReceiver)
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    val req = Request("/")
    intercept[TooLongMessageException] {
      await(client(req))
    }

    await(client.close())
    await(server.close())
  }

  test("server responds 500 if an invalid header is being served") {
    val service = new HttpService {
      def apply(request: Request): Future[Response] = {
        val response = Response()
        response.headerMap.add("foo", "|\f") // these are prohibited in N3
        Future.value(response)
      }
    }

    val server = serverImpl()
      .withStatsReceiver(NullStatsReceiver)
      .serve("localhost:*", service)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .withStatsReceiver(NullStatsReceiver)
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    val rep = await(client(Request("/")))
    assert(rep.status == Status.InternalServerError)
  }

  test("client respects MaxHeaderSize in response") {
    val ref = new ServiceFactoryRef(ServiceFactory.const(initService))

    val server = serverImpl()
      .withStatsReceiver(NullStatsReceiver)
      // we need to ignore header list size so we can examine behavior on the client-side
      .configured(EncoderIgnoreMaxHeaderListSize(true))
      .serve("localhost:*", ref)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .withMaxHeaderSize(1.kilobyte)
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    initClient(client)
    val svc = new Service[Request, Response] {
      def apply(request: Request) = {
        val response = Response()
        response.headerMap.set("foo", "*" * 1.kilobytes.bytes.toInt)
        Future.value(response)
      }
    }
    ref() = ServiceFactory.const(svc)

    val req = Request("/")
    intercept[TooLongMessageException] {
      await(client(req))
    }

    await(client.close())
    await(server.close())
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

  test("non-streaming clients can disable decompression") {
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
      .withDecompression(false)
      .withStatsReceiver(NullStatsReceiver)
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    val req = Request("/")
    req.headerMap.set("accept-encoding", "gzip")
    val rep = await(client(req))
    assert(rep.headerMap("content-encoding") == "gzip")
    assert(rep.contentString != "raw content")
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

  test("out of order client requests are OK") {
    val svc = new Service[Request, Response] {
      def apply(request: Request): Future[Response] = {
        Future.value(Response())
      }
    }

    val server = serverImpl()
      .serve("localhost:*", svc)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val factory = clientImpl()
      .newClient(s"${addr.getHostName}:${addr.getPort}", "client")

    val client1 = await(factory())
    val client2 = await(factory())

    await(client2(Request("/")))
    await(client1(Request("/")))

    await(client1.close())
    await(client2.close())
    await(factory.close())
    await(server.close())
  }

  test(s"$implName client handles cut connection properly") {
    val svc = Service.mk[Request, Response] { req: Request =>
      Future.value(Response())
    }
    val server1 = serverImpl()
      .serve("localhost:*", svc)
    val addr = server1.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .newService("%s:%d".format(addr.getHostName, addr.getPort), "client")

    val rep1 = await(client(Request("/")))

    assert(rep1.status == Status.Ok)
    await(server1.close())

    // we wait to ensure the client has been informed the connection has been dropped
    Thread.sleep(20)

    val server2 = serverImpl()
      .serve("localhost:%d".format(addr.getPort), svc)
    val rep2 = await(client(Request("/")))
    assert(rep2.status == Status.Ok)
  }

  test("Does not retry service acquisition many times when not using FactoryToService") {
    val svc = Service.mk[Request, Response] { req: Request =>
      Future.value(Response())
    }
    val sr = new InMemoryStatsReceiver
    val server = serverImpl()
      .serve("localhost:*", svc)
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .withStatsReceiver(sr)
      .newClient("%s:%d".format(addr.getHostName, addr.getPort), "client")

    val conn = await(client())

    assert(sr.counters.get(Seq("client", "retries", "requeues")) == None)

    await(conn.close())
    await(server.close())
    await(client.close())
  }

  private def testMethodBuilderTimeouts(
    stats: InMemoryStatsReceiver,
    server: ListeningServer,
    builder: MethodBuilder
  ): Unit = {
    // these should never complete within the timeout
    val shortTimeout: Service[Request, Response] = builder
      .withTimeoutPerRequest(5.millis)
      .newService("fast")

    intercept[IndividualRequestTimeoutException] {
      await(shortTimeout(Request()))
    }
    eventually {
      assert(stats.counter("a_label", "fast", "logical", "requests")() == 1)
      assert(stats.counter("a_label", "fast", "logical", "success")() == 0)
    }

    // these should always complete within the timeout
    val longTimeout: Service[Request, Response] = builder
      .withTimeoutPerRequest(5.seconds)
      .newService("slow")

    assert("ok" == await(longTimeout(Request())).contentString)
    eventually {
      assert(stats.counter("a_label", "slow", "logical", "requests")() == 1)
      assert(stats.counter("a_label", "slow", "logical", "success")() == 1)
    }

    await(Future.join(Seq(longTimeout.close(), shortTimeout.close())))
    await(server.close())
  }

  test(s"$implName: Graceful shutdown & draining") {
    val p = new Promise[Unit]
    @volatile var holdResponses = false

    val service = new HttpService {
      def apply(request: Request) = {
        val response = Response()
        response.contentString = request.uri

        if (holdResponses) p.map { _ =>
          response
        } else Future.value(response)
      }
    }

    val server = serverImpl().serve(new InetSocketAddress(0), service)
    val client = clientImpl().newService(
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )

    await(client(Request("/1")))

    holdResponses = true

    val rep = client(Request("/2"))

    Thread.sleep(100)

    server.close(5.seconds)

    Thread.sleep(100)
    p.setDone()

    assert(await(rep).contentString == "/2")

    val f = intercept[FailureFlags[_]] {
      await(client(Request("/3")))
    }

    // Connection refused
    assert(f.isFlagged(FailureFlags.Rejected))
  }

  test(implName + ": methodBuilder timeouts from Stack") {
    implicit val timer = HashedWheelTimer.Default
    val svc = new Service[Request, Response] {
      def apply(req: Request): Future[Response] = {
        Future.sleep(50.millis).before {
          val rep = Response()
          rep.setContentString("ok")
          Future.value(rep)
        }
      }
    }
    val server = serverImpl()
      .withStatsReceiver(NullStatsReceiver)
      .serve("localhost:*", svc)

    val stats = new InMemoryStatsReceiver()
    val client = clientImpl()
      .withStatsReceiver(stats)
      .configured(com.twitter.finagle.param.Timer(timer))
      .withLabel("a_label")
    val name = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
    val builder: MethodBuilder = client.methodBuilder(name)

    testMethodBuilderTimeouts(stats, server, builder)
  }

  test(implName + ": methodBuilder timeouts from ClientBuilder") {
    implicit val timer = HashedWheelTimer.Default
    val svc = new Service[Request, Response] {
      def apply(req: Request): Future[Response] = {
        Future.sleep(50.millis).before {
          val rep = Response()
          rep.setContentString("ok")
          Future.value(rep)
        }
      }
    }
    val server = serverImpl()
      .withStatsReceiver(NullStatsReceiver)
      .serve("localhost:*", svc)

    val stats = new InMemoryStatsReceiver()
    val client = clientImpl()
      .configured(com.twitter.finagle.param.Timer(timer))

    val clientBuilder = ClientBuilder()
      .reportTo(stats)
      .name("a_label")
      .stack(client)
      .dest(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])))

    val builder: MethodBuilder = MethodBuilder.from(clientBuilder)

    testMethodBuilderTimeouts(stats, server, builder)
  }

  private[this] def testMethodBuilderRetries(
    stats: InMemoryStatsReceiver,
    server: ListeningServer,
    builder: MethodBuilder
  ): Unit = {
    val retry500sClassifier: ResponseClassifier = {
      case ReqRep(_, Return(r: Response)) if r.statusCode / 100 == 5 =>
        ResponseClass.RetryableFailure
    }
    val ok503sClassifier: ResponseClassifier = {
      case ReqRep(_, Return(r: Response)) if r.statusCode == 503 =>
        ResponseClass.Success
    }

    val retry5xxs = builder
      .withRetryForClassifier(retry500sClassifier)
      .newService("5xx")

    val ok503s = builder
      .withRetryForClassifier(ok503sClassifier.orElse(retry500sClassifier))
      .newService("503")

    val req500 = Request()
    req500.contentString = "500"
    val req503 = Request()
    req503.contentString = "503"

    assert(500 == await(retry5xxs(req500)).statusCode)
    eventually {
      assert(stats.counter("a_label", "5xx", "logical", "requests")() == 1)
      assert(stats.counter("a_label", "5xx", "logical", "success")() == 0)
      assert(stats.stat("a_label", "5xx", "retries")() == Seq(2))
    }

    assert(503 == await(ok503s(req503)).statusCode)
    eventually {
      assert(stats.counter("a_label", "503", "logical", "requests")() == 1)
      assert(stats.counter("a_label", "503", "logical", "success")() == 1)
      assert(stats.stat("a_label", "503", "retries")() == Seq(0))
    }

    assert(500 == await(ok503s(req500)).statusCode)
    eventually {
      assert(stats.counter("a_label", "503", "logical", "requests")() == 2)
      assert(stats.counter("a_label", "503", "logical", "success")() == 1)
      assert(stats.stat("a_label", "503", "retries")() == Seq(0, 2))
    }

    await(Future.join(Seq(retry5xxs.close(), ok503s.close())))
    await(server.close())
  }

  test(implName + ": methodBuilder retries from Stack") {
    val svc = new Service[Request, Response] {
      def apply(req: Request): Future[Response] = {
        val rep = Response()
        rep.contentString = req.contentString
        req.contentString match {
          case "500" => rep.statusCode = 500
          case "503" => rep.statusCode = 503
          case _ => ()
        }
        Future.value(rep)
      }
    }
    val server = serverImpl()
      .withStatsReceiver(NullStatsReceiver)
      .serve("localhost:*", svc)

    val stats = new InMemoryStatsReceiver()
    val client = clientImpl()
      .withStatsReceiver(stats)
      .withLabel("a_label")

    val name = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
    val builder: MethodBuilder = client.methodBuilder(name)

    testMethodBuilderRetries(stats, server, builder)
  }

  test(implName + ": methodBuilder retries from ClientBuilder") {
    val svc = new Service[Request, Response] {
      def apply(req: Request): Future[Response] = {
        val rep = Response()
        rep.contentString = req.contentString
        req.contentString match {
          case "500" => rep.statusCode = 500
          case "503" => rep.statusCode = 503
          case _ => ()
        }
        Future.value(rep)
      }
    }
    val server = serverImpl()
      .withStatsReceiver(NullStatsReceiver)
      .serve("localhost:*", svc)

    val stats = new InMemoryStatsReceiver()
    val client = clientImpl()
    val clientBuilder = ClientBuilder()
      .reportTo(stats)
      .name("a_label")
      .stack(client)
      .dest(Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])))

    val builder: MethodBuilder = MethodBuilder.from(clientBuilder)

    testMethodBuilderRetries(stats, server, builder)
  }

  testIfImplemented(NoBodyMessage)(
    "response with status code {1xx, 204 and 304} must not have a message body nor Content-Length header field"
  ) {
    def check(resStatus: Status): Unit = {
      val svc = new Service[Request, Response] {
        def apply(request: Request) = {
          val response = Response(Version.Http11, resStatus)

          Future.value(response)
        }
      }
      val server = serverImpl()
        .serve("localhost:*", svc)

      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
      val client = clientImpl()
        .newService(s"${addr.getHostName}:${addr.getPort}", "client")

      val res = await(client(Request(Method.Get, "/")))
      assert(res.status == resStatus)
      assert(!res.isChunked)
      assert(res.content.isEmpty)
      assert(res.contentLength.isEmpty)
      await(client.close())
      await(server.close())
    }

    List(
      Status.Continue, /*Status.SwitchingProtocols,*/ Status.Processing,
      Status.NoContent,
      Status.NotModified
    ).foreach {
      check(_)
    }
  }

  testIfImplemented(NoBodyMessage)(
    "response with status code {1xx, 204 and 304} must not have a message body nor Content-Length header field" +
      "when non-empty body is returned"
  ) {
    def check(resStatus: Status): Unit = {
      val svc = new Service[Request, Response] {
        def apply(request: Request) = {
          val body = Buf.Utf8("some data")
          val response = Response(Version.Http11, resStatus)
          response.content = body

          Future.value(response)
        }
      }
      val server = serverImpl()
        .serve("localhost:*", svc)

      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
      val client = clientImpl()
        .newService(s"${addr.getHostName}:${addr.getPort}", "client")

      val res = await(client(Request(Method.Get, "/")))
      assert(res.status == resStatus)
      assert(!res.isChunked)
      assert(res.content.isEmpty)
      assert(res.contentLength.isEmpty)
      await(client.close())
      await(server.close())
    }

    List(
      Status.Continue, /*Status.SwitchingProtocols,*/ Status.Processing,
      Status.NoContent,
      Status.NotModified
    ).foreach {
      check(_)
    }
  }

  // We exclude SwitchingProtocols(101) since it should only be sent in response to a upgrade request
  List(Status.Continue, Status.Processing, Status.NoContent)
    .foreach { resStatus =>
      testIfImplemented(NoBodyMessage)(
        s"response with status code ${resStatus.code} must not have a message body nor " +
          "Content-Length header field when non-empty body with explicit Content-Length is returned"
      ) {
        val svc = new Service[Request, Response] {
          def apply(request: Request) = {
            val body = Buf.Utf8("some data")
            val response = Response(Version.Http11, resStatus)
            response.content = body
            response.headerMap.set(Fields.ContentLength, body.length.toString)

            Future.value(response)
          }
        }
        val server = serverImpl()
          .serve("localhost:*", svc)

        val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
        val client = clientImpl()
          .newService(s"${addr.getHostName}:${addr.getPort}", "client")

        val res = await(client(Request(Method.Get, "/")))
        assert(res.status == resStatus)
        assert(!res.isChunked)
        assert(res.length == 0)
        assert(res.contentLength.isEmpty)
        await(client.close())
        await(server.close())
      }
    }

  testIfImplemented(NoBodyMessage)(
    "response with status code 304 must not have a message body *BUT* Content-Length " +
      "header field when non-empty body with explicit Content-Length is returned"
  ) {
    val body = Buf.Utf8("some data")
    val svc = new Service[Request, Response] {
      def apply(request: Request) = {
        val response = Response(Version.Http11, Status.NotModified)
        response.content = body
        response.headerMap.set(Fields.ContentLength, body.length.toString)

        Future.value(response)
      }
    }
    val server = serverImpl()
      .serve("localhost:*", svc)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    val res = await(client(Request(Method.Get, "/")))
    assert(res.status == Status.NotModified)
    assert(!res.isChunked)
    assert(res.length == 0)
    assert(res.contentLength.contains(body.length.toLong))
    await(client.close())
    await(server.close())
  }

  test("ServerAdmissionControl doesn't filter requests with a chunked body") {
    val responseString = "a response"
    val svc = Service.mk[Request, Response] { _ =>
      val response = Response()
      response.contentString = responseString
      Future.value(response)
    }

    val nacked = new AtomicBoolean(false)
    val filter = new Filter.TypeAgnostic {
      override def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = new SimpleFilter[Req, Rep] {
        // nacks them all
        def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
          if (nacked.compareAndSet(false, true)) Future.exception(Failure.rejected)
          else service(request)
        }
      }
    }

    val server = serverImpl()
      .configured(ServerAdmissionControl.Filters(Some(Seq(_ => filter))))
      .serve("localhost:*", svc)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    // first, a request with a body
    val reqWithBody = Request(Method.Post, "/")
    reqWithBody.setChunked(true)
    val writer = reqWithBody.writer
    writer.write(Buf.Utf8("data")).before(writer.close())

    // Shouldn't be nacked
    assert(await(client(reqWithBody)).contentString == responseString)
    assert(!nacked.get)

    // Should be nacked the first time
    val reqWithoutBody = Request(Method.Get, "/")
    assert(await(client(reqWithoutBody)).contentString == responseString)
    assert(nacked.get)

    await(client.close())
    await(server.close())
  }

  test("ServerAdmissionControl can filter requests with the magic header") {
    val responseString = "a response"
    val svc = Service.mk[Request, Response] { _ =>
      val response = Response()
      response.contentString = responseString
      Future.value(response)
    }

    val nacked = new AtomicBoolean(false)
    val filter = new Filter.TypeAgnostic {
      override def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = new SimpleFilter[Req, Rep] {
        // nacks them all
        def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
          if (nacked.compareAndSet(false, true)) Future.exception(Failure.rejected)
          else service(request)
        }
      }
    }

    val server = serverImpl()
      .configured(ServerAdmissionControl.Filters(Some(Seq(_ => filter))))
      .serve("localhost:*", svc)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    // first, a request with a body
    val reqWithBody = Request(Method.Post, "/")
    reqWithBody.contentString = "not-empty"

    // Header should be there so we can nack it
    assert(await(client(reqWithBody)).contentString == responseString)
    assert(nacked.get)

    await(client.close())
    await(server.close())
  }
}
