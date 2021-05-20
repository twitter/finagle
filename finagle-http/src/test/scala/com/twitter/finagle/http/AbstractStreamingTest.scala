package com.twitter.finagle.http

import com.twitter.conversions.DurationOps._
import com.twitter.conversions.StorageUnitOps._
import com.twitter.finagle.http.{Status => HttpStatus}
import com.twitter.finagle.service.ConstantService
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Http => FinagleHttp, _}
import com.twitter.io._
import com.twitter.util._
import java.net.{InetSocketAddress, SocketAddress}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import org.scalatest.funsuite.AnyFunSuite

abstract class AbstractStreamingTest extends AnyFunSuite {

  protected def configureClient(
    client: FinagleHttp.Client,
    singletonPool: Boolean
  ): FinagleHttp.Client

  protected def configureServer(server: FinagleHttp.Server): FinagleHttp.Server

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
  def writeLots(writer: Writer[Buf], buf: Buf): Future[Unit] =
    writer.write(buf) before writeLots(writer, buf)

  class ClientCtx(singletonPool: Boolean = false) {
    @volatile var shouldFail = true
    val failure = new Promise[Unit]

    val server = startServer(echo)
    val client = connectWithModifier(server.boundAddress, singletonPool = singletonPool) {
      transport =>
        if (shouldFail) failure.ensure { await(transport.close()) }
        transport
    }

    val buf = Buf.Utf8(".")
    val req = get("/")
    val res = await(client(req))

    // Demonstrate normal operations by testing for a single echo'd chunk.
    await(req.writer.write(buf))
    assert(await(res.reader.read()) == Some(buf))

    // This request should queue in the service pool.
    shouldFail = false
    val req2 = get("abc")
    val res2 = client(req2)

    // Assert previously queued request is now processed, and not interrupted
    // midstream.
    def assertSecondRequestOk(): Unit = {
      try {
        await(res2.liftToTry) match {
          case Return(rsp) =>
            await(req2.writer.close())
            await(BufReader.readAll(rsp.reader))
          case Throw(e) =>
            fail(s"second request failed: $e")
        }
      } finally {
        await(Closable.all(server, client).close())
      }
    }
  }

  test("client: request stream fails on write")(new ClientCtx {
    // Simulate network failure by closing the transport.
    failure.setDone()

    intercept[ReaderDiscardedException] { await(writeLots(req.writer, buf)) }
    // We call read for the collating function to notice transport failure.
    intercept[ChannelException] { await(res.reader.read()) }

    assertSecondRequestOk()
  })

  test("client: response stream fails on read")(new ClientCtx(singletonPool = true) {
    assert(res2.poll == None)
    // Reader should be suspended in a reading state.
    val f = res.reader.read()
    assert(f.poll == None)

    // Simulate network failure by closing the transport.
    failure.setDone()

    // Assert reading state suspension is interrupted by transport closure.
    intercept[ChannelException] { await(f) }
    intercept[ReaderDiscardedException] { await(writeLots(req.writer, buf)) }

    assertSecondRequestOk()
  })

  test("client: server disconnect on pending response should fail request") {
    val reqReceived = Promise[Unit]()
    val server = startServer(Service.mk { _ =>
      reqReceived.setDone()
      Future.never
    })
    val client = connect(server.boundAddress)

    val resF = client(get("/"))

    await(reqReceived.before(server.close()))
    intercept[ChannelException] { await(resF) }
    await(client.close())

  }

  test("client: client closes transport after server disconnects") {
    val clientClosed = new Promise[Unit]
    val service = Service.mk[Request, Response] { _ => Future.value(Response()) }
    val server = startServer(service)
    val client = connectWithModifier(server.boundAddress) { transport =>
      clientClosed.become(transport.onClose.unit)
      transport
    }

    val res = await(client(get("/")))
    assert(await(res.reader.read()) == None)
    await(server.close())
    await(clientClosed)
  }

  test("client: fail request writer")(new ClientCtx(singletonPool = true) {
    assert(res2.poll.isEmpty)
    req.writer.fail(new Exception)
    assertSecondRequestOk()
  })

  test("client: discard respond reader")(new ClientCtx(singletonPool = true) {
    assert(res2.poll.isEmpty)
    res.reader.discard()
    assertSecondRequestOk()
  })

  test("server: request stream fails read") {
    val buf = Buf.Utf8(".")
    val n = new AtomicInteger(0)
    val readp = new Promise[Unit]
    val writer = new Pipe[Buf]()
    val req2Complete = Promise[Unit]()

    val service = new Service[Request, Response] {
      def apply(req: Request) = n.getAndIncrement() match {
        case 0 =>
          req.reader.read().unit.proxyTo(readp)
          Future.value(ok(writer))
        case _ =>
          val writer = new Pipe[Buf]()
          req2Complete.become(writer.write(buf).before(writer.close()))
          Future.value(ok(writer))
      }
    }

    val server = startServer(service)
    val client1 = connect(server.boundAddress, "client1")
    val client2 = connect(server.boundAddress, "client2")

    val req1 = get("/")
    val req2 = get("abc")
    val f1 = client1(req1)
    // note: while the server is configured with a max concurrency of 1,
    // the requests flow through the transport before that. this means
    // that these requests must be sequenced.
    val f2 = f1.flatMap { _ => client2(req2) }

    val res = await(f1)

    await(req2Complete.before(server.close()))

    intercept[ChannelClosedException] { await(readp) }
    intercept[ReaderDiscardedException] { await(writeLots(writer, buf)) }

    intercept[ChannelException] { await(res.reader.read()) }
    intercept[ReaderDiscardedException] { await(writeLots(req1.writer, buf)) }

    val res2 = await(f2)
    await(BufReader.readAll(res2.reader))
    await(Closable.all(server, client1, client2).close())
  }

  test("server: response stream fails write") {
    val buf = Buf.Utf8(".")
    val n = new AtomicInteger(0)
    val readp = new Promise[Unit]
    val writer = new Pipe[Buf]()
    val writep = new Promise[Unit]
    val req2Complete = Promise[Unit]()

    writeLots(writer, buf).proxyTo(writep)

    val service = new Service[Request, Response] {
      def apply(req: Request) = n.getAndIncrement() match {
        case 0 =>
          writep.ensure { req.reader.read().unit.proxyTo(readp) }
          Future.value(ok(writer))
        case _ =>
          val writer = new Pipe[Buf]()
          req2Complete.become(writer.write(buf).before(writer.close()))
          Future.value(ok(writer))
      }
    }

    val server = startServer(service)
    val client1 = connect(server.boundAddress, "client1")
    val client2 = connect(server.boundAddress, "client2")

    val req1 = get("/")
    val req2 = get("abc")
    val f1 = client1(req1)
    // note: while the server is configured with a max concurrency of 1,
    // the requests flow through the transport before that. this means
    // that these requests must be sequenced.
    val f2 = f1.flatMap { _ => client2(req2) }

    val res = await(f1)

    // Cut the server connections.
    await(req2Complete.before(server.close()))
    intercept[ReaderDiscardedException] { await(writep) }
    intercept[ChannelClosedException] { await(readp) }
    intercept[ChannelException] {
      val start = Time.now
      while ((Time.now - start) < 30.seconds) await(res.reader.read())
    }
    intercept[ReaderDiscardedException] { await(writeLots(req1.writer, buf)) }

    val res2 = await(f2)
    await(BufReader.readAll(res2.reader))
    await(Closable.all(client1, client2).close())
  }

  test("server: fail response writer") {
    val buf = Buf.Utf8(".")
    val n = new AtomicInteger(0)
    val failure = new Promise[Unit]

    val service = new Service[Request, Response] {
      def apply(req: Request) = n.getAndIncrement() match {
        case 0 =>
          val writer = new Pipe[Buf]()
          failure.ensure { writer.fail(new Exception) }
          Future.value(ok(writer))
        case _ =>
          val writer = new Pipe[Buf]()
          failure.ensure { writer.write(buf) ensure writer.close() }
          Future.value(ok(writer))
      }
    }

    val server = startServer(service)
    val client1 = connect(server.boundAddress, "client1")
    val client2 = connect(server.boundAddress, "client2")

    val req1 = get("/")
    val req2 = get("abc")
    val f1 = client1(req1)
    val f2 = f1.flatMap { _ => client2(req2) }

    val res = await(f1)

    failure.setDone()
    intercept[ChannelException] { await(res.reader.read()) }
    intercept[ReaderDiscardedException] { await(writeLots(req1.writer, buf)) }

    val res2 = await(f2)
    await(BufReader.readAll(res2.reader))
    await(Closable.all(server, client1, client2).close())
  }

  test("server: fail request reader") {
    val buf = Buf.Utf8(".")
    val n = new AtomicInteger(0)
    val failure = new Promise[Unit]

    val service = new Service[Request, Response] {
      def apply(req: Request) = n.getAndIncrement() match {
        case 0 =>
          val writer = new Pipe[Buf]()
          failure.ensure {
            req.reader.discard()
            writer.write(buf).ensure { writer.close() }
          }
          Future.value(ok(writer))
        case _ =>
          val writer = new Pipe[Buf]()
          failure.ensure { writer.write(buf).ensure { writer.close() } }
          Future.value(ok(writer))
      }
    }

    val server = startServer(service)
    val client1 = connect(server.boundAddress, "client1")
    val client2 = connect(server.boundAddress, "client2")

    val req1 = get("/")
    val req2 = get("abc")
    val f1 = client1(req1)
    val f2 = f1.flatMap { _ => client2(req2) }

    val res = await(f1)

    failure.setDone()
    intercept[ChannelException] { await(res.reader.read()) }
    intercept[ReaderDiscardedException] { await(writeLots(req1.writer, buf)) }

    val res2 = await(f2)
    await(BufReader.readAll(res2.reader))
    await(Closable.all(server, client1, client2).close())
  }

  test("server: empty buf doesn't close response stream") {
    val service = const(Seq(Buf.Utf8("hello"), Buf.Empty, Buf.Utf8("world")))
    val server = startServer(service)
    val client = connect(server.boundAddress, "client")
    val body = await(client(get("/")).flatMap(res => BufReader.readAll(res.reader)))
    assert(body == Buf.Utf8("helloworld"))
    await(Closable.all(server, client).close())
  }

  test("client: empty buf doesn't close request stream") {
    val server = startServer(echo)
    val client = connect(server.boundAddress, "client")
    val req = get("/")
    val res = await(client(req))
    await(for {
      _ <- req.writer.write(Buf.Utf8("hello"))
      _ <- req.writer.write(Buf.Empty)
      _ <- req.writer.write(Buf.Utf8("world"))
      _ <- req.writer.close()
    } yield ())
    val body = await(BufReader.readAll(res.reader))
    assert(body == Buf.Utf8("helloworld"))
    await(Closable.all(server, client).close())
  }

  test("end-to-end: server gets content for chunked request made to client with content length") {
    val svc = Service.mk[Request, Response] { req =>
      assert(req.contentString == "hello")
      Future.value(Response(req))
    }

    val server = startServer(svc)

    val writer = new Pipe[Buf]()
    val req = Request(Version.Http11, Method.Post, "/foo", writer)
    req.headerMap.put("Content-Length", "5")
    req.setChunked(true)

    val client = connect(server.boundAddress, "client")
    val res = client(req)
    await(writer.write(Buf.Utf8("hello")))
    writer.close()
    await(res)
    await(Closable.all(server, client).close())
  }

  test("end-to-end: client may process multiple streaming requests simultaneously") {
    val service = Service.mk[Request, Response] { req =>
      val writable = new Pipe[Buf]() // never gets closed
      Future.value(Response(req.version, HttpStatus.Ok, writable))
    }
    val server = startServer(service)
    val addr = server.boundAddress
    val client = connect(addr)
    try {
      val req0 = Request("/0")
      val rep0 = await(client(req0))
      assert(rep0.status == HttpStatus.Ok)
      assert(rep0.isChunked)

      val req1 = Request("/1")
      val rep1 = await(client(req1))
      assert(rep1.status == HttpStatus.Ok)
      assert(rep1.isChunked)
    } finally {
      await(Closable.all(client, server).close())
    }
  }

  test("server: inbound stream (reader) propagates closures initiated remotely") {
    val termination = new Promise[StreamTermination]
    val service = Service.mk[Request, Response] { req =>
      termination.become(req.reader.onClose)
      Future.never // never responds
    }
    val server = startServer(service)
    val addr = server.boundAddress
    val client = connect(addr)
    try {
      val req = Request("/")
      req.setChunked(true)

      client(req)
      req.writer.fail(new Exception())

      assert(await(termination.liftToTry).isThrow)
    } finally {
      await(Closable.all(client, server).close())
    }
  }

  test("server: outbound stream (writer) propagates closures initiated remotely") {
    val termination = new Promise[StreamTermination]
    val service = Service.mk[Request, Response] { _ =>
      val rep = Response()
      rep.setChunked(true)
      termination.become(rep.writer.onClose)
      Future.value(rep)
    }

    val server = startServer(service)
    val addr = server.boundAddress
    val client = connect(addr)
    try {
      val rep = await(client(Request("/")))
      rep.reader.discard()

      assert(!await(termination).isFullyRead)
    } finally {
      await(Closable.all(client, server).close())
    }
  }

  test("client: inbound stream (reader) propagates closures initiated remotely") {
    val stream = new Promise[Writer[Buf]]
    val service = Service.mk[Request, Response] { req =>
      val rep = Response()
      rep.setChunked(true)
      stream.setValue(rep.writer)
      Future.value(rep)
    }
    val server = startServer(service)
    val addr = server.boundAddress
    val client = connect(addr)
    try {
      val rep = await(client(Request("/")))
      await(stream).fail(new Exception())

      assert(await(rep.reader.onClose.liftToTry).isThrow)
    } finally {
      await(Closable.all(client, server).close())
    }
  }

  test("client: outbound stream (writer) propagates closures initiated remotely") {
    val service = Service.mk[Request, Response] { req =>
      req.reader.discard()
      Future.never
    }

    val server = startServer(service)
    val addr = server.boundAddress
    val client = connect(addr)
    try {
      val req = Request("/")
      req.setChunked(true)
      client(req)

      assert(!await(req.writer.onClose).isFullyRead)
    } finally {
      await(Closable.all(client, server).close())
    }
  }

  def startServer(service: Service[Request, Response]): ListeningServer = {
    configureServer(FinagleHttp.server)
      .withStreaming(0.bytes) // no aggregation
      .withLabel("server")
      .serve(new InetSocketAddress(0), service)
  }

  def connect(
    addr: SocketAddress,
    name: String = "client",
    singletonPool: Boolean = false
  ): Service[Request, Response] = connectWithModifier(addr, name, singletonPool)(identity)

  def connectWithModifier(
    addr: SocketAddress,
    name: String = "client",
    singletonPool: Boolean = false
  )(
    mod: Modifier
  ): Service[Request, Response] = {
    configureClient(FinagleHttp.client, singletonPool)
      .withStreaming(0.bytes) // no aggregation
      .configured(ClientEndpointer.TransportModifier(mod))
      .newService(Name.bound(Address(addr.asInstanceOf[InetSocketAddress])), name)
  }

  def closingOnceTransport(closed: Future[Unit]): Modifier = {
    val setFail = new AtomicBoolean(false)

    val mod: Modifier = { transport: Transport[Any, Any] =>
      if (!setFail.getAndSet(true)) closed.ensure {
        await(transport.close())
      }
      transport
    }
    mod
  }
}

object StreamingTest {

  type Modifier = Transport[Any, Any] => Transport[Any, Any]

  def await[A](f: Future[A]): A = Await.result(f, 30.seconds)

  val echo = new Service[Request, Response] {
    def apply(req: Request): Future[Response] = Future.value(ok(req.reader))
  }

  def const(bufs: Seq[Buf]): Service[Request, Response] =
    new Service[Request, Response] {
      private def drain(writer: Writer[Buf], bs: Seq[Buf]): Future[Unit] = bs match {
        case Nil => Future.Done
        case head +: tail => writer.write(head).before(drain(writer, tail))
      }

      def apply(req: Request): Future[Response] = {
        val writer = new Pipe[Buf]()
        drain(writer, bufs).before(writer.close)
        Future.value(ok(writer))
      }
    }

  val neverRespond = new ConstantService[Request, Response](Future.never)

  def get(uri: String): Request = {
    val req = Request(uri)
    req.setChunked(true)
    req
  }

  def ok(readerIn: Reader[Buf]): Response = {
    val res = Response(Version.Http11, HttpStatus.Ok, readerIn)
    res.headerMap.set("Connection", "close")
    res
  }
}
