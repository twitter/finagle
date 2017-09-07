package com.twitter.finagle.http

import com.twitter.conversions.time._
import com.twitter.finagle.{Http => FinagleHttp, _}
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.server.Listener
import com.twitter.finagle.service.ConstantService
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.io.{Buf, Reader, Writer}
import com.twitter.util.{Await, Closable, Future, Promise, Return, Throw}
import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import org.scalatest.FunSuite

abstract class AbstractStreamingTest extends FunSuite {

  def impl: FinagleHttp.HttpImpl
  def configureClient: FinagleHttp.Client => FinagleHttp.Client = identity
  def configureServer: FinagleHttp.Server => FinagleHttp.Server = identity

  import StreamingTest._

  // Enumerated Failure Cases
  // ------------------------
  //
  // Caused by network failure:
  //
  // 1. Client: request stream fails on write
  // 2. Client: response stream fails on read
  // 3. Server: request stream fails on read
  // 4. Server: response stream fails on write
  //
  // Application initiated failure:
  //
  // 5. Client: fails request writer
  // 6. Client: discards response reader
  // 7. Server: fails response writer
  // 8. Server: discards request reader

  // We call write repeatedly for `streamChunks` to *be sure* to notice
  // transport failure.
  def writeLots(writer: Writer, buf: Buf): Future[Unit] =
    writer.write(buf) before writeLots(writer, buf)

  class ClientCtx {
    @volatile var shouldFail = true
    val failure = new Promise[Unit]

    val server = startServer(echo, identity)
    val client = connect(server.boundAddress, transport => {
      if (shouldFail) failure.ensure { transport.close() }
      transport
    })

    val buf = Buf.Utf8(".")
    val req = get("/")
    val res = await(client(req))

    // Demonstrate normal operations by testing for a single echo'd chunk.
    await(req.writer.write(buf))
    assert(await(res.reader.read(1)) == Some(buf))

    // This request should queue in the service pool.
    shouldFail = false
    val req2 = get("abc")
    val res2 = client(req2)

    // This is flaky. The assertion has been moved to the tests which use res2, and those have been
    // marked as flaky.
    //assert(!res2.isDefined)

    // Assert previously queued request is now processed, and not interrupted
    // midstream.
    def assertSecondRequestOk() = {
      await(res2.liftToTry) match {
        case Return(rsp) =>
          val reader = rsp.reader
          await(req2.writer.close())
          await(Reader.readAll(reader))
          await(Closable.all(server, client).close())
        case Throw(e) =>
          fail(s"second request failed: $e")
      }
    }
  }

  test("client: request stream fails on write")(new ClientCtx {
    // Simulate network failure by closing the transport.
    failure.setDone()

    intercept[Reader.ReaderDiscarded] { await(writeLots(req.writer, buf)) }
    // We call read for the collating function to notice transport failure.
    intercept[ChannelException] { await(res.reader.read(1)) }

    assertSecondRequestOk()
  })

  if (!sys.props.contains("SKIP_FLAKY"))
    test("client: response stream fails on read")(new ClientCtx {
      assert(!res2.isDefined)
      // Reader should be suspended in a reading state.
      val f = res.reader.read(1)
      assert(!f.isDefined)

      // Simulate network failure by closing the transport.
      failure.setDone()

      // Assert reading state suspension is interrupted by transport closure.
      intercept[ChannelException] { await(f) }
      intercept[Reader.ReaderDiscarded] { await(writeLots(req.writer, buf)) }

      assertSecondRequestOk()
    })

  test("client: server disconnect on pending response should fail request") {
    val failure = new Promise[Unit]
    val (mod, closable) = closingTransport(failure)
    val server = startServer(neverRespond, mod)
    val client = connect(server.boundAddress, identity)

    val resF = client(get("/"))

    failure.setDone()
    intercept[ChannelException] { await(resF) }

    await(closable.close())
    await(client.close())
    await(server.close())
  }

  test("client: client closes transport after server disconnects") {
    val serverClose, clientClosed = new Promise[Unit]
    val service = Service.mk[Request, Response] { req =>
      Future.value(Response())
    }
    val (mod, closable) = closingTransport(serverClose)
    val server = startServer(service, mod)
    val client = connect(server.boundAddress, transport => {
      clientClosed.become(transport.onClose.unit)
      transport
    })

    val res = await(client(get("/")))
    assert(await(res.reader.read(1)) == None)
    serverClose.setDone()
    await(clientClosed)
    await(closable.close())
  }

  if (!sys.props.contains("SKIP_FLAKY"))
    test("client: fail request writer")(new ClientCtx {
      assert(!res2.isDefined)
      val exc = new Exception
      req.writer.fail(exc)

      res.reader.discard()

      assertSecondRequestOk()
    })

  if (!sys.props.contains("SKIP_FLAKY"))
    test("client: discard respond reader")(new ClientCtx {
      assert(!res2.isDefined)
      res.reader.discard()
      assertSecondRequestOk()
    })

  test("server: request stream fails read") {
    val buf = Buf.Utf8(".")
    val n = new AtomicInteger(0)
    val failure = new Promise[Unit]
    val readp = new Promise[Unit]
    val writer = Reader.writable()

    val service = new Service[Request, Response] {
      def apply(req: Request) = n.getAndIncrement() match {
        case 0 =>
          req.reader.read(1).unit proxyTo readp
          Future.value(ok(writer))
        case _ =>
          val writer = Reader.writable()
          failure.ensure { writer.write(buf) ensure writer.close() }
          Future.value(ok(writer))
      }
    }

    val (mod, closable) = closingOnceTransport(failure)
    val server = startServer(service, mod)
    val client1 = connect(server.boundAddress, identity, "client1")
    val client2 = connect(server.boundAddress, identity, "client2")

    val req1 = get("/")
    val req2 = get("abc")
    val f1 = client1(req1)
    // note: while the server is configured with a max concurrency of 1,
    // the requests flow through the transport before that. this means
    // that these requests must be sequenced.
    val f2 = f1.flatMap { _ =>
      client2(req2)
    }

    val res = await(f1)

    failure.setDone()
    intercept[ChannelClosedException] { await(readp) }
    intercept[Reader.ReaderDiscarded] { await(writeLots(writer, buf)) }

    intercept[ChannelException] { await(res.reader.read(1)) }
    intercept[Reader.ReaderDiscarded] { await(writeLots(req1.writer, buf)) }

    val res2 = await(f2)
    await(Reader.readAll(res2.reader))
    Closable.all(server, client1, client2, closable).close()
  }

  if (!sys.props.contains("SKIP_FLAKY"))
    test("server: response stream fails write") {
      val buf = Buf.Utf8(".")
      val n = new AtomicInteger(0)
      val failure = new Promise[Unit]
      val readp = new Promise[Unit]
      val writer = Reader.writable()
      val writep = new Promise[Unit]
      failure.before(writeLots(writer, buf)).proxyTo(writep)

      val service = new Service[Request, Response] {
        def apply(req: Request) = n.getAndIncrement() match {
          case 0 =>
            writep ensure req.reader.read(1).unit proxyTo readp
            Future.value(ok(writer))
          case _ =>
            val writer = Reader.writable()
            failure.ensure { writer.write(buf) ensure writer.close() }
            Future.value(ok(writer))
        }
      }

      val (mod, closable) = closingOnceTransport(failure)
      val server = startServer(service, mod)
      val client1 = connect(server.boundAddress, identity, "client1")
      val client2 = connect(server.boundAddress, identity, "client2")

      val req1 = get("/")
      val req2 = get("abc")
      val f1 = client1(req1)
      // note: while the server is configured with a max concurrency of 1,
      // the requests flow through the transport before that. this means
      // that these requests must be sequenced.
      val f2 = f1.flatMap { _ =>
        client2(req2)
      }

      val res = await(f1)

      failure.setDone()
      intercept[Reader.ReaderDiscarded] { await(writep) }
      // This really should be ChannelClosedException, perhaps we're too
      // indiscriminatory by calling discard on any error in the dispatcher.
      intercept[Reader.ReaderDiscarded] { await(readp) }
      intercept[ChannelException] { await(res.reader.read(1)) }
      intercept[Reader.ReaderDiscarded] { await(writeLots(req1.writer, buf)) }

      val res2 = await(f2)
      await(Reader.readAll(res2.reader))
      Closable.all(server, client1, client2, closable).close()
    }

  test("server: fail response writer") {
    val buf = Buf.Utf8(".")
    val n = new AtomicInteger(0)
    val failure = new Promise[Unit]

    val service = new Service[Request, Response] {
      def apply(req: Request) = n.getAndIncrement() match {
        case 0 =>
          val writer = Reader.writable()
          failure.ensure { writer.fail(new Exception) }
          Future.value(ok(writer))
        case _ =>
          val writer = Reader.writable()
          failure.ensure { writer.write(buf) ensure writer.close() }
          Future.value(ok(writer))
      }
    }

    val server = startServer(service, identity)
    val client1 = connect(server.boundAddress, identity, "client1")
    val client2 = connect(server.boundAddress, identity, "client2")

    val req1 = get("/")
    val req2 = get("abc")
    val f1 = client1(req1)
    val f2 = f1.flatMap { _ =>
      client2(req2)
    }

    val res = await(f1)

    failure.setDone()
    intercept[ChannelException] { await(res.reader.read(1)) }
    intercept[Reader.ReaderDiscarded] { await(writeLots(req1.writer, buf)) }

    val res2 = await(f2)
    await(Reader.readAll(res2.reader))
    Closable.all(server, client1, client2).close()
  }

  test("server: fail request reader") {
    val buf = Buf.Utf8(".")
    val n = new AtomicInteger(0)
    val failure = new Promise[Unit]

    val service = new Service[Request, Response] {
      def apply(req: Request) = n.getAndIncrement() match {
        case 0 =>
          failure.ensure { req.reader.discard() }
          val writer = Reader.writable()
          failure.ensure { writer.write(buf) ensure writer.close() }
          Future.value(ok(writer))
        case _ =>
          val writer = Reader.writable()
          failure.ensure { writer.write(buf) ensure writer.close() }
          Future.value(ok(writer))
      }
    }

    val server = startServer(service, identity)
    val client1 = connect(server.boundAddress, identity, "client1")
    val client2 = connect(server.boundAddress, identity, "client2")

    val req1 = get("/")
    val req2 = get("abc")
    val f1 = client1(req1)
    val f2 = f1.flatMap { _ =>
      client2(req2)
    }

    val res = await(f1)

    failure.setDone()
    intercept[ChannelException] { await(res.reader.read(1)) }
    intercept[Reader.ReaderDiscarded] { await(writeLots(req1.writer, buf)) }

    val res2 = await(f2)
    await(Reader.readAll(res2.reader))
    Closable.all(server, client1, client2).close()
  }

  test("end-to-end: server gets content for chunked request made to client with content length") {
    val svc = Service.mk[Request, Response] { req =>
      assert(req.contentString == "hello")
      Future.value(Response(req))
    }

    val server = startServer(svc, identity)

    val writer = Reader.writable()
    val req = Request(Version.Http11, Method.Post, "/foo", writer)
    req.headerMap.put("Content-Length", "5")
    req.setChunked(true)

    val client = connect(server.boundAddress, identity, "client")
    val res = client(req)
    await(writer.write(Buf.Utf8("hello")))
    writer.close()
    await(res)
    Closable.all(server, client).close()
  }

  test("end-to-end: client may process multiple streaming requests simultaneously") {
    val service = Service.mk[Request, Response] { req =>
      val writable = Reader.writable() // never gets closed
      Future.value(Response(req.version, Status.Ok, writable))
    }
    val server = startServer(service, identity)
    val addr = server.boundAddress
    val client = connect(addr, identity)
    try {
      val req0 = Request("/0")
      val rep0 = await(client(req0))
      assert(rep0.status == Status.Ok)
      assert(rep0.isChunked)

      val req1 = Request("/1")
      val rep1 = await(client(req1))
      assert(rep1.status == Status.Ok)
      assert(rep1.isChunked)
    } finally {
      client.close()
      server.close()
    }
  }

  def startServer(service: Service[Request, Response], mod: Modifier): ListeningServer = {
    val modifiedImpl = impl.copy(listener = modifiedListenerFn(mod, impl.listener))
    configureServer(FinagleHttp.server)
      .withStreaming(true)
      .configured(modifiedImpl)
      .withLabel("server")
      .serve(new InetSocketAddress(0), service)
  }

  def connect(
    addr: SocketAddress,
    mod: Modifier,
    name: String = "client"
  ): Service[Request, Response] = {
    val modifiedImpl = impl.copy(transporter = modifiedTransporterFn(mod, impl.transporter))
    configureClient(FinagleHttp.client)
      .withStreaming(true)
      .configured(modifiedImpl)
      .newService(Name.bound(Address(addr.asInstanceOf[InetSocketAddress])), name)
  }

  def closingTransport(closed: Future[Unit]): (Modifier, Closable) = {
    (
      (transport: Transport[Any, Any]) => {
        closed.ensure { transport.close() }
        transport
      },
      Closable.nop
    )
  }

  def closingOnceTransport(closed: Future[Unit]): (Modifier, Closable) = {
    val setFail = new AtomicBoolean(false)

    (
      (transport: Transport[Any, Any]) => {
        if (!setFail.getAndSet(true)) closed.ensure { transport.close() }
        transport
      },
      Closable.nop
    )
  }
}

object StreamingTest {

  def await[A](f: Future[A]): A = Await.result(f, 30.seconds)

  val echo = new Service[Request, Response] {
    def apply(req: Request) = Future.value(ok(req.reader))
  }
  val neverRespond = new ConstantService[Request, Response](Future.never)

  def get(uri: String) = {
    val req = Request(uri)
    req.setChunked(true)
    req
  }

  def ok(readerIn: Reader) = {
    val res = Response(Version.Http11, Status.Ok, readerIn)
    res.headerMap.set("Connection", "close")
    res
  }

  type Modifier = Transport[Any, Any] => Transport[Any, Any]

  def modifiedTransporterFn(
    mod: Modifier,
    fn: Stack.Params => SocketAddress => Transporter[Any, Any, TransportContext]
  ): Stack.Params => SocketAddress => Transporter[Any, Any, TransportContext] = {
    params: Stack.Params =>
      { addr =>
        val underlying = fn(params)(addr)
        new Transporter[Any, Any, TransportContext] {
          def apply(): Future[Transport[Any, Any]] = {
            underlying().map(mod)
          }

          def remoteAddress: SocketAddress = underlying.remoteAddress
        }
      }
  }

  def modifiedListenerFn(
    mod: Modifier,
    fn: Stack.Params => Listener[Any, Any, TransportContext]
  ): Stack.Params => Listener[Any, Any, TransportContext] = { params: Stack.Params =>
    val underlying = fn(params)
    new Listener[Any, Any, TransportContext] {
      def listen(
        addr: SocketAddress
      )(
        serveTransport: Transport[Any, Any] { type Context <: TransportContext } => Unit
      ): ListeningServer = underlying.listen(addr)(mod.andThen(serveTransport))
    }
  }
}
