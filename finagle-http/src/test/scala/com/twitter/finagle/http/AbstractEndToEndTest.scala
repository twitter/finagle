package com.twitter.finagle.http

import com.twitter.conversions.StorageUnitOps._
import com.twitter.conversions.DurationOps._
import com.twitter.finagle
import com.twitter.finagle._
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.context.{Contexts, Deadline, Retries}
import com.twitter.finagle.filter.ServerAdmissionControl
import com.twitter.finagle.http.codec.context.LoadableHttpContext
import com.twitter.finagle.http.service.HttpResponseClassifier
import com.twitter.finagle.http.{Status => HttpStatus}
import com.twitter.finagle.http2.param.EncoderIgnoreMaxHeaderListSize
import com.twitter.finagle.liveness.{FailureAccrualFactory, FailureDetector}
import com.twitter.finagle.service._
import com.twitter.finagle.stats.{
  InMemoryStatsReceiver,
  LoadedStatsReceiver,
  NullStatsReceiver,
  StandardStatsReceiver
}
import com.twitter.finagle.tracing.Trace
import com.twitter.finagle.util.DefaultTimer
import com.twitter.io.{Buf, BufReader, Pipe, Reader, ReaderDiscardedException, Writer}
import com.twitter.util._
import io.netty.buffer.PooledByteBufAllocator
import java.io.{PrintWriter, StringWriter}
import java.net.{InetAddress, InetSocketAddress}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import org.scalactic.source.Position
import org.scalatest.{BeforeAndAfter, OneInstancePerTest, Tag}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import scala.language.reflectiveCalls
import org.scalatest.funsuite.AnyFunSuite

// copied from finagle-base-http HttpContext test for marshalling external custom context type via
// LoadService. We have an end-to-end test here as well
case class NameContext(name: String)

object NameContext
    extends Contexts.broadcast.Key[NameContext]("com.twitter.finagle.http.NameContext") {
  def marshal(ctxVal: NameContext): Buf = {
    Buf.ByteArray.Owned(ctxVal.name.getBytes())
  }

  def tryUnmarshal(buf: Buf): Try[NameContext] = {
    Try {
      NameContext(
        new String(Buf.ByteArray.Owned.extract(buf))
      )
    }
  }
}

// This class definition must be included into jar's resources via the file,
// `c.t.f.http.codec.context.LoadableHttpContext`, under META-INF/services
// directory so that LoadService can pickup this definition at runtime. See
// the resources of this target as an example.
class LoadedNameContext extends LoadableHttpContext {
  type ContextKeyType = NameContext
  val key: Contexts.broadcast.Key[NameContext] = NameContext
}

abstract class AbstractEndToEndTest
    extends AnyFunSuite
    with BeforeAndAfter
    with Eventually
    with IntegrationPatience
    with OneInstancePerTest {

  sealed trait Feature
  object ClientAbort extends Feature
  object NoBodyMessage extends Feature
  object MaxHeaderSize extends Feature

  var saveBase: Dtab = Dtab.empty
  var statsRecv: InMemoryStatsReceiver = new InMemoryStatsReceiver()

  before {
    saveBase = Dtab.base
    Dtab.base = Dtab.read("/foo=>/bar; /baz=>/biz")
    statsRecv = new InMemoryStatsReceiver()
  }

  after {
    Dtab.base = saveBase
    statsRecv = new InMemoryStatsReceiver()
  }

  type HttpService = Service[Request, Response]
  type HttpTest = (HttpService => HttpService) => Unit

  def await[T](f: Future[T]): T = Await.result(f, 30.seconds)

  def drip(w: Writer[Buf]): Future[Unit] = w.write(buf("*")) before drip(w)
  def buf(msg: String): Buf = Buf.Utf8(msg)
  def implName: String
  def skipWholeTest: Boolean = false
  def clientImpl(): finagle.Http.Client
  def serverImpl(): finagle.Http.Server
  def initClient(client: HttpService): Unit = {}
  def initService: HttpService = Service.mk { _: Request =>
    Future.exception(new Exception("boom!"))
  }
  def featureImplemented(feature: Feature): Boolean
  def testIfImplemented(feature: Feature)(name: String)(testFn: => Unit): Unit = {
    if (!featureImplemented(feature)) ignore(name)(testFn) else test(name)(testFn)
  }

  /**
   * Read `n` number of bytes from the bytestream represented by `r`.
   */
  def readNBytes(n: Int, r: Reader[Buf]): Future[Buf] = {
    def loop(left: Buf): Future[Buf] = n - left.length match {
      case x if x > 0 =>
        r.read().flatMap {
          case Some(right) => loop(left.concat(right))
          case None => Future.value(left)
        }
      case _ => Future.value(left)
    }

    loop(Buf.Empty)
  }

  private def requestWith(status: HttpStatus): Request =
    Request("/", ("statusCode", status.code.toString))

  private val statusCodeSvc = new HttpService {
    def apply(request: Request): Future[Response] = {
      val statusCode = request.getIntParam("statusCode", HttpStatus.BadRequest.code)
      Future.value(Response(HttpStatus.fromCode(statusCode)))
    }
  }

  override def test(
    testName: String,
    testTags: Tag*
  )(
    testFun: => Any
  )(
    implicit pos: Position
  ): Unit = {
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
      .configured(FailureDetector.Param(FailureDetector.NullConfig))
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
      .withStreaming(8.kilobytes)
      .withLabel("server")
      .withStatsReceiver(statsRecv)
      .serve("localhost:*", ref)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .withStreaming(8.kilobytes)
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
      request.headerMap.add("header", "a" * 8192)
      val response = await(client(request))
      assert(response.status == HttpStatus.RequestHeaderFieldsTooLarge)
      await(client.close())
    }

    test(implName + ": with default client-side ResponseClassifier") {
      val client = connect(statusCodeSvc)

      await(client(requestWith(HttpStatus.Ok)))
      assert(statsRecv.counters(Seq("client", "requests")) == 1)
      assert(statsRecv.counters(Seq("client", "success")) == 1)

      await(client(requestWith(HttpStatus.ServiceUnavailable)))
      assert(statsRecv.counters(Seq("client", "requests")) == 2)
      // by default 500s are treated as unsuccessful
      assert(statsRecv.counters(Seq("client", "success")) == 1)

      await(client.close())
    }

    test(implName + ": with default server-side ResponseClassifier") {
      val client = connect(statusCodeSvc)

      await(client(requestWith(HttpStatus.Ok)))
      assert(statsRecv.counters(Seq("server", "requests")) == 1)
      assert(statsRecv.counters(Seq("server", "success")) == 1)

      await(client(requestWith(HttpStatus.ServiceUnavailable)))
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
      assert(response.status == HttpStatus.InternalServerError)
      await(client.close())
    }

    if (!sys.props.contains("SKIP_FLAKY_TRAVIS"))
      test(implName + ": return 413s for fixed-length requests with too large payloads") {
        val service = new HttpService {
          def apply(request: Request) = Future.value(Response())
        }
        val client = connect(service)

        val tooBig = Request("/")
        tooBig.content = Buf.ByteArray.Owned(new Array[Byte](300))

        val justRight = Request("/")
        justRight.content = Buf.ByteArray.Owned(new Array[Byte](200))

        assert(await(client(tooBig)).status == HttpStatus.RequestEntityTooLarge)
        assert(await(client(justRight)).status == HttpStatus.Ok)
        await(client.close())
      }

    if (!sys.props.contains("SKIP_FLAKY_TRAVIS"))
      test(
        implName +
          ": return 413s for chunked requests which stream too much data"
      ) {
        val service = new HttpService {
          def apply(request: Request) = Future.value(Response())
        }
        val client = connect(service)

        val justRight = Request("/")
        assert(await(client(justRight)).status == HttpStatus.Ok)

        val tooMuch = Request("/")
        tooMuch.setChunked(true)
        val w = tooMuch.writer
        w.write(buf("a" * 1000)).before(w.close)
        val res = client(tooMuch)
        Await.ready(res, 5.seconds)

        res.poll.get match {
          case Return(resp) =>
            assert(resp.status == HttpStatus.RequestEntityTooLarge)
          case Throw(_: ChannelClosedException) =>
            ()
          case t =>
            fail(s"expected a 413 or a ChannelClosedException, saw $t")
        }

        await(client.close())
      }
  }

  def standardBehaviour(connect: HttpService => HttpService): Unit = {

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

    test(implName + ": external contexts via loadservice") {
      // lets reuse the external Name context type defined in finagle-base-http
      val name = NameContext("foo")
      val service = new HttpService {
        def apply(request: Request) = {
          val nameCtx = Contexts.broadcast.get(NameContext).get
          assert(nameCtx.name == name.name)

          val response = Response(request)
          Future.value(response)
        }
      }

      Contexts.broadcast.let(NameContext, name) {
        val client = connect(service)
        val res = await(client(Request("/")))
        assert(res.status == HttpStatus.Ok)
        await(client.close())
      }
    }

    test(implName + ": (no) dtab") {
      val service = new HttpService {
        def apply(request: Request) = {
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

    test(implName + ": Dtab.limited does not propagate") {
      val service = new HttpService {
        def apply(request: Request) = {
          val response = Response(request)
          response.contentString = "%d".format(Dtab.limited.length)
          Future.value(response)
        }
      }

      val client = connect(service)

      Dtab.unwind {
        Dtab.limited ++= Dtab.read("/a=>/b; /c=>/d")

        val res = await(client(Request("/")))
        assert(res.contentString == "0")
      }

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
        assert(res.status == HttpStatus.Ok)
        await(client.close())
      }
    }

    if (!sys.props.contains("SKIP_FLAKY"))
      testIfImplemented(ClientAbort)(implName + ": client abort") {
        import com.twitter.conversions.DurationOps._
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

        // the payloadsize filter measures response sizes as a side-effect (.respond)
        // so for h2c we sometimes see the warmup request's response payload despite
        // clearing the stats
        val clientResponse = statsRecv.stat("client", "response_payload_bytes")()
        assert(clientResponse == Seq(20.0f) || clientResponse == Seq(0f, 20.0f))
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
      assert(await(f2).status == HttpStatus.Ok)

      await(client.close())
    }

    test("Retryable nacks surface as finagle Failures") {
      val svc = Service.mk[Request, Response] { _ =>
        Future.exception(Failure.RetryableNackFailure)
      }
      val client = nonStreamingConnect(svc)
      val f = intercept[Failure] { await(client(Request())) }
      assert(!f.isFlagged(FailureFlags.Retryable))
      assert(f.isFlagged(FailureFlags.Rejected))
      await(client.close())
    }

    test("Non-retryable nacks surface as finagle Failures") {
      val svc = Service.mk[Request, Response] { _ =>
        Future.exception(Failure.NonRetryableNackFailure)
      }
      val client = nonStreamingConnect(svc)
      val f = intercept[Failure] { await(client(Request())) }
      assert(f.isFlagged(FailureFlags.NonRetryable))
      assert(f.isFlagged(FailureFlags.Rejected))
      await(client.close())
    }

    test(implName + ": aggregates trailers when streams are aggregated") {
      val service = new HttpService {
        def apply(req: Request): Future[Response] = {
          assert(req.trailers.size == 2)
          assert(req.trailers("foo") == "bar")
          assert(req.trailers("bar") == "baz")

          Future.value(Response())
        }
      }

      val client = connect(service)

      val req = Request()
      req.setChunked(true)

      val rep = client(req)

      val out = for {
        _ <- req.chunkWriter.write(Chunk.last(HeaderMap("foo" -> "bar", "bar" -> "baz")))
        _ <- req.chunkWriter.close()
      } yield ()

      await(out.before(rep))
      await(client.close())
    }
  }

  def streaming(connect: HttpService => HttpService): Unit = {
    test(s"$implName (streaming)" + ": stream") {
      def service(r: Reader[Buf]) = new HttpService {
        def apply(request: Request) = {
          val response = Response.chunked(Version.Http11, HttpStatus.Ok, r)
          Future.value(response)
        }
      }

      val writer = new Pipe[Buf]()
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
      val Buf.Utf8(actual) = await(BufReader.readAll(response.reader))
      assert(actual == "goodbyeworld")
      await(client.close())
    }

    test(s"$implName (streaming): aggregates responses that must not have a body") {
      val service = new HttpService {
        def apply(request: Request): Future[Response] = {
          val resp = Response()
          resp.status = HttpStatus.NoContent
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
          val response = EnrichedResponse(Response(Version.Http11, HttpStatus.Ok))
          response.setChunked(true)

          response.writer.write(Buf.Utf8("hello")) before {
            Future.sleep(Duration.fromSeconds(3))(DefaultTimer) before {
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
      val Buf.Utf8(actual) = await(BufReader.readAll(response.reader))
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

      val content = await(client(req).flatMap { rep => BufReader.readAll(rep.reader) })
      assert(Buf.Utf8.unapply(content).get == "raw content")
      await(client.close())
    }

    test(s"$implName (streaming)" + ": symmetric reader and getContent") {
      val s = Service.mk[Request, Response] { req =>
        BufReader.readAll(req.reader).map { buf =>
          assert(buf == Buf.Utf8("hello"))
          if (!req.isChunked) {
            assert(req.contentString == "hello")
          }
          val resp = Response(req)
          resp.content = buf
          resp
        }
      }
      val req = Request()
      req.contentString = "hello"
      req.headerMap.put("Content-Length", "5")
      val client = connect(s)
      val res = await(client(req))

      val buf = await(BufReader.readAll(res.reader))
      assert(buf == Buf.Utf8("hello"))
      assert(res.contentString == "hello")
    }

    test(
      s"$implName (streaming): transport closure propagates to request stream reader"
    ) {
      val p = new Promise[Buf]
      val s = Service.mk[Request, Response] { req =>
        p.become(BufReader.readAll(req.reader))
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
      val s = Service.mk[Request, Response] { _ => Future.value(Response()) }
      val client = connect(s)
      val req = Request()
      req.setChunked(true)
      await(client(req))
      await(client.close())
      intercept[ReaderDiscardedException] { await(drip(req.writer)) }
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
            Some(c) <- req.reader.read()
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

      val contentf = resf flatMap { res => BufReader.readAll(res.reader) }
      assert(await(contentf) == Buf.Utf8("hello"))

      // drip should terminate because the request is discarded.
      intercept[ReaderDiscardedException] { await(drip(req.writer)) }
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
          val body = BufReader.readAll(req.reader)

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

    test(s"$implName (streaming)" + ": measure chunk payload size") {
      val svc = Service.mk[Request, Response] { req =>
        req.reader.read()
        val rep = Response()
        rep.setChunked(true)
        rep.writer.write(Buf.Utf8("01234"))
        Future.value(rep)
      }
      val req = Request()
      req.setChunked(true)
      req.writer.write(Buf.Utf8("0123456789"))
      val client = connect(svc)
      val response = await(client(req))
      response.reader.read()

      eventually {
        assert(statsRecv.stat("client", "stream", "request", "chunk_payload_bytes")() == Seq(10f))
        assert(statsRecv.stat("client", "stream", "response", "chunk_payload_bytes")() == Seq(5f))
        assert(statsRecv.stat("server", "stream", "request", "chunk_payload_bytes")() == Seq(10f))
        assert(statsRecv.stat("server", "stream", "response", "chunk_payload_bytes")() == Seq(5f))
      }
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

    test(implName + ": streaming requests can't be retried") {
      val failService = new HttpService {
        def apply(req: Request): Future[Response] =
          req.reader.read().flatMap { _ =>
            Future.exception(Failure("try again", FailureFlags.Retryable | FailureFlags.Rejected))
          }
      }

      val client = connect(failService)

      val e = intercept[FailureFlags[_]] {
        val out = new Pipe[Buf]
        val req = Request(Version.Http11, Method.Post, "/", out)
        val rep = client(req)

        await(out.write(Buf.Utf8("foo")))
        await(rep)
      }

      assert(e.isFlagged(FailureFlags.Rejected))

      eventually {
        assert(
          !statsRecv.counters.contains(Seq("client", "retries", "requeues")) ||
            statsRecv.counters(Seq("client", "retries", "requeues")) == 0)
        assert(statsRecv.counters(Seq("client", "failures")) == 1)
        assert(statsRecv.counters(Seq("client", "requests")) == 1)
      }

      await(client.close())
    }

    test(implName + ": streaming session bi-directionally transmit trailing headers") {
      val service = new HttpService {
        def apply(req: Request): Future[Response] = {
          val rep = Response()
          rep.setChunked(true)

          for {
            ts <- req.chunkReader.read().map(_.get.trailers)
            _ <- rep.chunkWriter.write(Chunk.last(ts.add("bar", "baz")))
            _ <- req.chunkReader.read()
            _ <- rep.chunkWriter.close()
          } yield ()

          Future.value(rep)
        }
      }

      val client = connect(service)
      val req = Request()
      req.setChunked(true)

      val rep = await(client(req))
      val trailersIn = HeaderMap.apply("foo" -> "bar")

      val out = for {
        _ <- req.chunkWriter.write(Chunk.last(trailersIn))
        _ <- req.chunkWriter.close()
      } yield ()

      await(out)

      val trailersOut = await(rep.chunkReader.read()).get.trailers
      assert(await(rep.chunkReader.read()).isEmpty)

      assert(trailersOut.size == 2)
      assert(trailersOut("foo") == "bar")
      assert(trailersOut("bar") == "baz")

      await(client.close())
    }

    test(implName + ": invalid trailer causes server to hang up") {
      val observed = new Promise[HeaderMap]
      val service = new HttpService {
        def apply(req: Request): Future[Response] = {
          observed.become(req.chunkReader.read().map(_.get.trailers))
          Future.value(Response())
        }
      }

      val client = connect(service)
      val req = Request()
      req.setChunked(true)

      val rep = await(client(req))
      assert(rep.status == HttpStatus.Ok)

      val trailers = HeaderMap.newHeaderMap
      illegalHeaders.foreach { case (k, v) => trailers.addUnsafe(k, v) }

      val out = for {
        _ <- req.chunkWriter.write(Chunk.last(trailers))
        _ <- req.chunkWriter.close()
      } yield ()

      await(out)
      intercept[ChannelException](await(observed))

      await(client.close())
    }
  }

  def tracing(connect: HttpService => HttpService): Unit = {
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

  test(
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

  test(
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

  if (!sys.props.contains("SKIP_FLAKY_TRAVIS"))
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
      assert(
        statsRecv.counters(Seq(clientName, "retries", "requeues")) == failureAccrualFailures - 1
      )
      assert(
        statsRecv.counters(Seq(clientName, "failures", "restartable")) == failureAccrualFailures
      )
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

    assert(statsRecv.counters(Seq(clientName, "failure_accrual", "removals")) == 0)
    assert(statsRecv.counters(Seq(clientName, "retries", "requeues")) == 0)
    assert(!statsRecv.counters.contains(Seq(clientName, "failures", "restartable")))
    assert(statsRecv.counters(Seq(clientName, "failures")) == 1)
    assert(statsRecv.counters(Seq(clientName, "requests")) == 1)
    await(Closable.all(client, server).close())
  }

  test("Client-side ResponseClassifier based on status code") {
    val classifier = HttpResponseClassifier {
      case (_, r: Response) if r.status == HttpStatus.ServiceUnavailable =>
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

    val rep1 = await(client(requestWith(HttpStatus.Ok)))
    assert(statsRecv.counters(Seq("client", "requests")) == 1)
    assert(statsRecv.counters(Seq("client", "success")) == 1)

    val rep2 = await(client(requestWith(HttpStatus.ServiceUnavailable)))

    assert(statsRecv.counters(Seq("client", "requests")) == 2)
    assert(statsRecv.counters(Seq("client", "success")) == 1)

    await(client.close())
    await(server.close())
  }

  test("server-side ResponseClassifier based on status code") {
    val classifier = HttpResponseClassifier {
      case (_, r: Response) if r.status == HttpStatus.ServiceUnavailable =>
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

    await(client(requestWith(HttpStatus.Ok)))
    assert(statsRecv.counters(Seq("server", "requests")) == 1)
    assert(statsRecv.counters(Seq("server", "success")) == 1)

    await(client(requestWith(HttpStatus.ServiceUnavailable)))
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

  val illegalHeaders = for {
    k <- Seq("FgR", "a\fb")
    v <- Seq("a\u000bb", "a\fb") // vtab
  } yield k -> v

  test("server rejects illegal headers with a 400") {
    val service = nonStreamingConnect(Service.mk(_ => Future.value(Response())))

    illegalHeaders.foreach {
      case (k, v) =>
        val badRequest = Request()
        badRequest.headerMap.addUnsafe(k, v)
        val resp = await(service(badRequest))
        assert(resp.status == HttpStatus.BadRequest)
    }

    await(service.close())
  }

  test("server rejects illegal trailers with a 400") {
    val service = nonStreamingConnect(Service.mk(_ => Future.value(Response())))

    illegalHeaders.foreach {
      case (k, v) =>
        val badRequest = Request()
        badRequest.setChunked(true)

        val trailers = HeaderMap.newHeaderMap
        trailers.addUnsafe(k, v)

        for {
          _ <- badRequest.chunkWriter.write(Chunk.last(trailers))
          _ <- badRequest.chunkWriter.close()
        } yield ()

        val resp = await(service(badRequest))

        assert(resp.status == HttpStatus.BadRequest)
    }

    await(service.close())
  }

  test("client rejects illegal headers with an exception") {
    val current = new AtomicReference("a" -> "b")
    val service = nonStreamingConnect(Service.mk { _ =>
      val resp = Response()
      val (k, v) = current.get
      resp.headerMap.addUnsafe(k, v)
      Future.value(resp)
    })

    illegalHeaders.foreach { kv =>
      current.set(kv)
      intercept[Exception](await(service(Request())))
    }

    await(service.close())
  }

  test("client rejects illegal trailer with an exception") {
    val current = new AtomicReference("a" -> "b")
    val service = nonStreamingConnect(Service.mk { _ =>
      val rep = Response()
      rep.setChunked(true)

      val (k, v) = current.get
      val trailers = HeaderMap.newHeaderMap
      trailers.addUnsafe(k, v)

      for {
        _ <- rep.chunkWriter.write(Chunk.last(trailers))
        _ <- rep.chunkWriter.close()
      } yield ()

      Future.value(rep)
    })

    illegalHeaders.foreach { kv =>
      current.set(kv)
      intercept[Exception](await(service(Request())))
    }

    await(service.close())
  }

  test("obs-fold sequences are 'fixed' when received by clients") {
    val service = nonStreamingConnect(Service.mk { _ =>
      val resp = Response()
      resp.headerMap.addUnsafe("foo", "biz\r\n baz")
      Future.value(resp)
    })

    val resp = await(service(Request()))
    assert(resp.headerMap.get("foo") == Some("biz baz"))
    await(service.close())
  }

  test("obs-fold sequences are 'fixed' when received by servers") {
    val service = nonStreamingConnect(Service.mk { req =>
      val resp = Response()
      req.headerMap.get("foo").foreach { v => resp.contentString = v }
      Future.value(resp)
    })

    val req = Request()
    req.headerMap.addUnsafe("foo", "biz\r\n baz")
    val resp = await(service(req))
    assert(resp.contentString == "biz baz")
    await(service.close())
  }

  test("client throws InvalidUriException with non-ascii character is present in uri") {
    val expected = "/DSC02175拷貝.jpg"
    // we shouldn't hit the service code, but if we do it is because something was incorrectly
    // filtered out in the netty pipeline and this will help debug.
    val service = Service.mk[Request, Response] {
      case req if req.uri == expected =>
        Future.value(Response())
      case req =>
        Future.exception(new Exception(s"Unexpected request URI: ${req.uri}"))
    }
    val server = serverImpl().withStatsReceiver(NullStatsReceiver).serve("localhost:*", service)
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]

    val client = clientImpl()
      .withStatsReceiver(NullStatsReceiver)
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    try {
      intercept[InvalidUriException] {
        await(client(Request(expected)))
      }
    } finally {
      await(client.close())
      await(server.close())
    }
  }

  test("client throws InvalidUriException with illegal character encoding is present in uri") {
    val expected = "/example/routing/json/1%%"

    // we shouldn't hit the service code, but if we do it is because something was incorrectly
    // filtered out in the netty pipeline and this will help debug.
    val service = Service.mk[Request, Response] {
      case req if req.uri == expected => Future.value(Response())
      case req => Future.exception(new Exception(s"Unexpected request URI: ${req.uri}"))
    }
    val server = serverImpl().withStatsReceiver(NullStatsReceiver).serve("localhost:*", service)
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]

    val client = clientImpl()
      .withStatsReceiver(NullStatsReceiver)
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    try {
      intercept[InvalidUriException] {
        await(client(Request(expected)))
      }
    } finally {
      await(client.close())
      await(server.close())
    }
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
    assert(rep.status == HttpStatus.InternalServerError)

  }

  testIfImplemented(MaxHeaderSize)("client respects MaxHeaderSize in response") {
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

  test("removing the compressor works") {
    val svc = new Service[Request, Response] {
      def apply(request: Request) = {
        val response = Response()
        response.contentString = "raw content"
        Future.value(response)
      }
    }
    val server = serverImpl()
      .withCompressionLevel(0)
      .serve("localhost:*", svc)

    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .newService(s"${addr.getHostName}:${addr.getPort}", "client")

    val req = Request("/")
    val rep = await(client(req))
    assert(rep.contentString == "raw content")
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
    val svc = Service.mk[Request, Response] { _: Request => Future.value(Response()) }
    val server1 = serverImpl()
      .serve("localhost:*", svc)
    val addr = server1.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .newService("%s:%d".format(addr.getHostName, addr.getPort), "client")

    val rep1 = await(client(Request("/")))

    assert(rep1.status == HttpStatus.Ok)
    await(server1.close())

    // we wait to ensure the client has been informed the connection has been dropped
    Thread.sleep(20)

    val server2 = serverImpl()
      .serve("localhost:%d".format(addr.getPort), svc)
    val rep2 = await(client(Request("/")))
    assert(rep2.status == HttpStatus.Ok)
  }

  test("Does not retry service acquisition many times when not using FactoryToService") {
    val svc = Service.mk[Request, Response] { _: Request => Future.value(Response()) }
    val sr = new InMemoryStatsReceiver
    val server = serverImpl()
      .serve("localhost:*", svc)
    val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
    val client = clientImpl()
      .withStatsReceiver(sr)
      .newClient("%s:%d".format(addr.getHostName, addr.getPort), "client")

    val conn = await(client())

    assert(sr.counters(Seq("client", "retries", "requeues")) == 0)

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

        if (holdResponses) p.map { _ => response }
        else Future.value(response)
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
    import DefaultTimer.Implicit
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
      .configured(com.twitter.finagle.param.Timer(DefaultTimer))
      .withLabel("a_label")
    val name = Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress]))
    val builder: MethodBuilder = client.methodBuilder(name)

    testMethodBuilderTimeouts(stats, server, builder)
  }

  test(implName + ": methodBuilder timeouts from ClientBuilder") {
    import DefaultTimer.Implicit
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
      .configured(com.twitter.finagle.param.Timer(DefaultTimer))

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

  if (!sys.props.contains("SKIP_FLAKY_TRAVIS")) // Maybe netty4 http/2 only
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

  if (!sys.props.contains("SKIP_FLAKY_TRAVIS")) // Maybe netty4 http/2 only
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
    def check(resStatus: HttpStatus): Unit = {
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
      HttpStatus.Continue, /*HttpStatus.SwitchingProtocols,*/ HttpStatus.Processing,
      HttpStatus.NoContent,
      HttpStatus.NotModified
    ).foreach {
      check(_)
    }
  }

  testIfImplemented(NoBodyMessage)(
    "response with status code {1xx, 204 and 304} must not have a message body nor Content-Length header field" +
      "when non-empty body is returned"
  ) {
    def check(resStatus: HttpStatus): Unit = {
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
      HttpStatus.Continue, /*HttpStatus.SwitchingProtocols,*/ HttpStatus.Processing,
      HttpStatus.NoContent,
      HttpStatus.NotModified
    ).foreach {
      check(_)
    }
  }

  // We exclude SwitchingProtocols(101) since it should only be sent in response to a upgrade request
  List(HttpStatus.Continue, HttpStatus.Processing, HttpStatus.NoContent)
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
        val response = Response(Version.Http11, HttpStatus.NotModified)
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
    assert(res.status == HttpStatus.NotModified)
    assert(!res.isChunked)
    assert(res.length == 0)
    assert(res.contentLength.contains(body.length.toLong))
    await(client.close())
    await(server.close())
  }

  if (!sys.props.contains("SKIP_FLAKY"))
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
        .configured(ServerAdmissionControl.Filters(Map("server_ac" -> (_ => filter))))
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

  if (!sys.props.contains("SKIP_FLAKY"))
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
        .configured(ServerAdmissionControl.Filters(Map("server_ac" -> (_ => filter))))
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

  test(s"$implName: server read timeouts") {
    val service = new HttpService {
      def apply(request: Request): Future[Response] = {
        val response = Response()
        Future.value(response).delayed(200.milliseconds)(DefaultTimer.Implicit)
      }
    }

    val server = serverImpl().withTransport
      .readTimeout(300.milliseconds)
      .serve(new InetSocketAddress(0), service)

    val client = clientImpl().newService(
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )

    await(client(Request("/1")))
    Thread.sleep(200)
    await(client(Request("/2")))
    Thread.sleep(200)
    await(client(Request("/3")))
    await(client.close())
  }

  test(s"$implName: streaming client read timeouts") {
    val service = new HttpService {
      def apply(request: Request): Future[Response] = {
        Future.value(Response())
      }
    }

    val server = serverImpl().withTransport
      .readTimeout(100.milliseconds)
      .serve(new InetSocketAddress(0), service)

    val client = clientImpl()
      .withStreaming(true)
      .newService(
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        "client"
      )

    val req = Request("/1")
    req.setChunked(true)

    val flags = intercept[FailureFlags[_]] {
      await(client(req))
    }

    assert(!flags.isFlagged(FailureFlags.Retryable))
    await(client.close())
    await(server.close())
  }

  test(s"$implName: streaming client and server read timeouts") {
    val service = new HttpService {
      def apply(request: Request): Future[Response] = {
        Future.value(Response())
      }
    }

    val server = serverImpl()
      .withStreaming(true)
      .withTransport.readTimeout(100.milliseconds)
      .serve(new InetSocketAddress(0), service)

    val client = clientImpl()
      .withStreaming(true)
      .newService(
        Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
        "client"
      )

    val req = Request("/1")
    req.setChunked(true)

    def writeLoop(remaining: Int): Future[Unit] = {
      if (remaining <= 0) Future.Done
      else {
        Future
          .sleep(50.milliseconds)(DefaultTimer.Implicit)
          .before(req.writer.write(Buf.Utf8("foo")))
          .before(writeLoop(remaining - 1))
      }
    }

    // Start writing data. This should be fine.
    writeLoop(4)
    assert(await(client(req)).status == HttpStatus.Ok)

    await(client.close())
    await(server.close())
  }

  test("Proxy large streaming responses") {
    val messageSize = 28.megabytes

    def getSocket(server: ListeningServer): String = {
      val addr = server.boundAddress.asInstanceOf[InetSocketAddress]
      s"${addr.getHostName}:${addr.getPort}"
    }

    val originServer: ListeningServer = {
      val chunk = Buf.Utf8("a" * 16.kilobytes.bytes.toInt)
      val service = Service.mk { _: Request =>
        val resp = Response()
        resp.setChunked(true)
        def drip(sentBytes: Int): Future[Unit] = {
          if (sentBytes >= messageSize.bytes) resp.writer.close()
          else {
            val toSend = math.min(messageSize.bytes - sentBytes, chunk.length).toInt
            resp.writer
              .write(chunk.slice(0, toSend))
              .before(drip(sentBytes + toSend))
          }
        }
        // Start writing.
        drip(sentBytes = 0)

        Future.value(resp)
      }

      serverImpl()
        .serve("localhost:*", service)
    }

    val proxyServer: ListeningServer = {
      val client =
        clientImpl()
          .withStreaming(true)
          .withStatsReceiver(NullStatsReceiver)
          .newService(getSocket(originServer))

      serverImpl()
        .withStreaming(true)
        .serve("localhost:*", client)
    }

    val userClient: Service[Request, Response] = {
      clientImpl()
        .withMaxResponseSize(50.megabytes)
        .newService(getSocket(proxyServer))
    }

    val resp = Await.result(userClient(Request()), 15.seconds)
    assert(resp.content.length == messageSize.bytes)
    await(Closable.all(userClient, proxyServer, originServer).close())
  }

  test(s"$implName: standard server metrics") {
    val service = new HttpService {
      def apply(request: Request): Future[Response] = {
        if (request.uri == "/3") Future.value(Response(HttpStatus.InternalServerError))
        else Future.value(Response())
      }
    }
    val builtinSr = new InMemoryStatsReceiver()
    val sr = new InMemoryStatsReceiver()
    val loadedSr = LoadedStatsReceiver.self
    LoadedStatsReceiver.self = builtinSr
    StandardStatsReceiver.serverCount.set(0)
    val server = serverImpl()
      .withStatsReceiver(sr)
      .withResponseClassifier(ResponseClassifier.Default)
      .serve(new InetSocketAddress(0), service)

    val client = clientImpl().newService(
      Name.bound(Address(server.boundAddress.asInstanceOf[InetSocketAddress])),
      "client"
    )
    await(client(Request("/1")))
    await(client(Request("/2")))
    await(client(Request("/3")))

    assert(builtinSr.counter("standard-service-metric-v1", "srv", "requests")() == 3)
    assert(sr.counter("requests")() == 3)
    assert(sr.counter("success")() == 3)
    assert(
      builtinSr
        .counter("standard-service-metric-v1", "srv", "http", "server-0", "requests")() == 3)
    assert(
      builtinSr
        .counter("standard-service-metric-v1", "srv", "http", "server-0", "success")() == 2)
    LoadedStatsReceiver.self = loadedSr
    await(client.close())
    await(server.close())
  }
}
