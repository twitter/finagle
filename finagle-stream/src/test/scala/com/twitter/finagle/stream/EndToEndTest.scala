package com.twitter.finagle.stream

import com.twitter.concurrent._
import com.twitter.conversions.time._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.{Service, ServiceProxy, TooManyConcurrentRequestsException}
import com.twitter.io.Buf
import com.twitter.util._
import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import java.util.concurrent.Executors
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.channel._
import org.jboss.netty.handler.codec.http._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.language.postfixOps

@RunWith(classOf[JUnitRunner])
class EndToEndTest extends FunSuite {
  import Bijections._

  type Request = Int
  type StreamService = Service[Request, StreamResponse]

  implicit val requestRequestType: RequestType[Request] =
    new RequestType[Request] {
      def canonize(req: Request) = StreamRequest(StreamRequest.Method.Get, "/")
      def specialize(req: StreamRequest) = 1
    }

  val codec = new Stream[Request]

  case class MyStreamResponse(
    info: StreamResponse.Info,
    messages: Offer[Buf],
    error: Offer[Throwable]
  ) extends StreamResponse {

    val released = new Promise[Unit]
    def release() = released.updateIfEmpty(Return(()))
  }

  class MyService(response: StreamResponse) extends StreamService {
    def apply(request: Request) = Future.value(response)
  }

  class WorkItContext(){
    val streamRequest = 1
    val httpRequest = from(StreamRequest(StreamRequest.Method.Get, "/")): HttpRequest
    val info = StreamResponse.Info(Version(1, 1), StreamResponse.Status(200), Nil)
    val messages = new Broker[Buf]
    val error = new Broker[Throwable]
    val serverRes = MyStreamResponse(info, messages.recv, error.recv)
  }

  def workIt(what: String)(mkClient: (MyStreamResponse) => (StreamService, SocketAddress)) {
    test("Streams %s: writes from the server arrive on the client's channel".format(what)) {
      val c = new WorkItContext()
      import c._
      val (client, _) = mkClient(serverRes)
      val clientRes = Await.result(client(streamRequest), 1.second)
      var result = ""
      val latch = new CountDownLatch(1)
      (clientRes.error?) ensure {
        Future { latch.countDown() }
      }

      clientRes.messages.foreach { case Buf.Utf8(str) =>
        result += str
      }

      messages !! Buf.Utf8("1")
      messages !! Buf.Utf8("2")
      messages !! Buf.Utf8("3")
      error !! EOF

      latch.within(1.second)
      assert(result == "123")
      client.close()
    }

    test("Streams %s: writes from the server are queued before the client responds".format(what)) {
      val c = new WorkItContext()
      import c._
      val (client, _) = mkClient(serverRes)
      val clientRes = Await.result(client(streamRequest), 1.second)
      messages !! Buf.Utf8("1")
      messages !! Buf.Utf8("2")
      messages !! Buf.Utf8("3")

      val latch = new CountDownLatch(3)
      var result = ""
      clientRes.messages.foreach { case Buf.Utf8(str) =>
        result += str
        latch.countDown()
      }

      latch.within(1.second)
      error !! EOF
      assert(result == "123")
      client.close()
    }

    test("Streams %s: the client does not admit concurrent requests".format(what)) {
      val c = new WorkItContext()
      import c._
      val (client, _) = mkClient(serverRes)
      val clientRes = Await.result(client(streamRequest), 15.seconds)
      assert(client(streamRequest).poll match {
        case Some(Throw(_: TooManyConcurrentRequestsException)) => true
        case _ => false
      })
      client.close()
    }

    if (!sys.props.contains("SKIP_FLAKY"))
    test("Streams %s: the server does not admit concurrent requests".format(what)) {
      val c = new WorkItContext()
      import c._
      val (client, address) = mkClient(serverRes)
      // The finagle client, by nature, doesn't allow for this, so
      // we need to go through the trouble of establishing our own
      // pipeline.

      val recvd = new Broker[ChannelEvent]
      val bootstrap = new ClientBootstrap(
        new NioClientSocketChannelFactory(
          Executors.newCachedThreadPool(),
          Executors.newCachedThreadPool()))
      bootstrap.setPipelineFactory(new ChannelPipelineFactory {
        override def getPipeline() = {
          val pipeline = Channels.pipeline()
          pipeline.addLast("httpCodec", new HttpClientCodec)
          pipeline.addLast("recvd", new ChannelUpstreamHandler {
            override def handleUpstream(ctx: ChannelHandlerContext, e: ChannelEvent) {
              val keep = e match {
                case se: ChannelStateEvent =>
                  se.getState == ChannelState.OPEN
                case _: WriteCompletionEvent => false
                case _ => true
              }
              if (keep) recvd ! e
            }
          })
          pipeline
        }
      })

      val connectFuture = bootstrap
        .connect(address)
        .awaitUninterruptibly()
      assert(connectFuture.isSuccess)
      val channel = connectFuture.getChannel

      // first request is accepted
      assert(channel
        .write(httpRequest)
        .awaitUninterruptibly()
        .isSuccess)

      messages !! Buf.Utf8("chunk1")

      assert(Await.result(recvd?, 1.second) match {
        case e: ChannelStateEvent =>
          e.getState == ChannelState.OPEN && (java.lang.Boolean.TRUE equals e.getValue)
        case _ => false
      })

      assert(Await.result(recvd?, 1.second) match {
        case m: MessageEvent =>
          m.getMessage match {
            case res: HttpResponse => res.isChunked
            case _ => false
          }
        case _ => false
      })

      assert(Await.result(recvd?, 1.second) match {
        case m: MessageEvent =>
          m.getMessage match {
            case res: HttpChunk => !res.isLast  // get "chunk1"
            case _ => false
          }
        case _ => false
      })

      // The following requests should be ignored
      assert(channel
        .write(httpRequest)
        .awaitUninterruptibly()
        .isSuccess)

      // the streaming should continue
      messages !! Buf.Utf8("chunk2")

      assert(Await.result(recvd?, 1.second) match {
        case m: MessageEvent =>
          m.getMessage match {
            case res: HttpChunk => !res.isLast  // get "chunk2"
            case _ => false
          }
        case _ => false
      })

      error !! EOF
      assert(Await.result(recvd?, 1.second) match {
        // Flaky because ChannelEvent can be an ExceptionEvent of
        // "java.io.IOException: Connection reset by peer". Uncomment the
        // following line to observe.
        // case e: ExceptionEvent => throw new Exception(e.getCause)
        case m: MessageEvent =>
          m.getMessage match {
            case res: HttpChunkTrailer => res.isLast
            case _ => false
          }
        case _ => false
      })

      // And finally it's closed.
      assert(Await.result(recvd?, 1.second) match {
        case e: ChannelStateEvent =>
          e.getState == ChannelState.OPEN && (java.lang.Boolean.FALSE equals e.getValue)
        case _ => false
      })

      bootstrap.releaseExternalResources()
      channel.close()
    }

    test("Streams %s: server ignores channel buffer messages after channel close".format(what)) {
      val c = new WorkItContext()
      import c._
      val (client, address) = mkClient(serverRes)

      val clientRes = Await.result(client(streamRequest), 1.second)
      var result = ""
      val latch = new CountDownLatch(1)

      (clientRes.error?) ensure {
        Future { latch.countDown() }
      }

      clientRes.messages.foreach { case Buf.Utf8(str) =>
        result += str
      }

      FuturePool.unboundedPool {
        messages !! Buf.Utf8("12")
        messages !! Buf.Utf8("23")
        error !! EOF
        messages !! Buf.Utf8("34")
      }

      latch.within(1.second)
      assert(result == "1223")
    }
  }

  workIt("straight") { serverRes =>
    val server = ServerBuilder()
      .codec(codec)
      .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
      .name("Streams")
      .build(new MyService(serverRes))
    val address = server.boundAddress
    val factory = ClientBuilder()
      .codec(codec)
      .hosts(Seq(address))
      .hostConnectionLimit(1)
      .buildFactory()

    val underlying = Await.result(factory())
    val client = new ServiceProxy[Request, StreamResponse](underlying) {
      override def close(deadline: Time) =
        Closable.all(underlying, server, factory).close(deadline)
    }

    (client, address)
  }

  workIt("proxy") { serverRes =>
    val server = ServerBuilder()
      .codec(codec)
      .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
      .name("streamserver")
      .build(new MyService(serverRes))

    val serverClient = ClientBuilder()
      .codec(codec)
      .hosts(Seq(server.boundAddress))
      .hostConnectionLimit(1)
      .build()

    val proxy = ServerBuilder()
      .codec(codec)
      .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
      .name("streamproxy")
      .build(serverClient)

    val factory = ClientBuilder()
      .codec(codec)
      .hosts(Seq(proxy.boundAddress))
      .hostConnectionLimit(1)
      .buildFactory()

    val underlying = Await.result(factory())
    val client = new ServiceProxy[Request, StreamResponse](underlying) {
      override def close(deadline: Time) =
        Closable.all(server, serverClient, proxy, factory).close(deadline)
    }

    (client, proxy.boundAddress)
  }

  test("Streams: headers") {
    val s = Service.mk { (req: StreamRequest) =>
      val errors = new Broker[Throwable]
      errors ! EOF
      Future.value(new StreamResponse {
        val info = StreamResponse.Info(req.version, StreamResponse.Status(200), req.headers)
        def messages = new Broker[Buf].recv
        def error = errors.recv
        def release() = errors !! EOF
      })
    }

    val addr = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val server = ServerBuilder()
      .codec(Stream[StreamRequest]())
      .bindTo(addr)
      .name("s")
      .build(s)

    val client = ClientBuilder()
      .codec(Stream[StreamRequest]())
      .hosts(Seq(server.boundAddress))
      .hostConnectionLimit(1)
      .build()

    val headers = Seq(Header("a", "b"), Header("c", "d"))
    val req = StreamRequest(StreamRequest.Method.Get, "/", headers = headers)
    val res = Await.result(client(req), 1.second)
    assert(headers.forall(res.info.headers.contains), s"$headers not found in ${res.info.headers}")

    Closable.all(client, server).close()
  }

  test("Streams: delay release until complete response") {
    @volatile var count: Int = 0
    val c = new WorkItContext()
    import c.{synchronized => _sync, _}

    val server = ServerBuilder()
      .codec(codec)
      .bindTo(new InetSocketAddress(InetAddress.getLoopbackAddress, 0))
      .name("Streams")
      .build((new MyService(serverRes)).map { r: Request =>
        synchronized { count += 1 }
        r
      })
    val client = ClientBuilder()
      .codec(codec)
      .hosts(Seq(server.boundAddress))
      .hostConnectionLimit(1)
      .retries(2)
      .build()

    val res = Await.result(client(streamRequest), 1.second)
    assert(count == 1)
    val f2 = client(streamRequest)
    assert(f2.poll.isEmpty)  // because of the host connection limit

    messages !! Buf.Utf8("1")
    assert((res.messages??) == Buf.Utf8("1"))
    assert(count == 1)
    error !! EOF
    res.release()
    val res2 = Await.result(f2, 1.second)
    assert(count == 2)
    res2.release()

    Closable.all(client, server)
  }
}
