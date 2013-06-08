package com.twitter.finagle.stream

import com.twitter.concurrent._
import com.twitter.conversions.time._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.{Service, ServiceProxy, TooManyConcurrentRequestsException}
import com.twitter.util._
import java.net.{InetSocketAddress, SocketAddress}
import java.nio.charset.Charset
import java.util.concurrent.Executors
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory
import org.jboss.netty.channel.{ChannelEvent, ChannelHandlerContext, ChannelPipelineFactory, ChannelState, ChannelStateEvent, ChannelUpstreamHandler, Channels, MessageEvent, WriteCompletionEvent}
import org.jboss.netty.handler.codec.http._
import org.specs.SpecificationWithJUnit

class EndToEndSpec extends SpecificationWithJUnit {
  case class MyStreamResponse(
      httpResponse: HttpResponse,
      messages: Offer[ChannelBuffer],
      error: Offer[Throwable])
      extends StreamResponse
  {
    val released = new Promise[Unit]
    def release() = released.updateIfEmpty(Return(()))
  }

  class MyService(response: StreamResponse)
    extends Service[HttpRequest, StreamResponse]
  {
    def apply(request: HttpRequest) = Future.value(response)
  }

  "Streams" should {
    val httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
    val httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    val messages = new Broker[ChannelBuffer]
    val error = new Broker[Throwable]
    val serverRes = MyStreamResponse(httpResponse, messages.recv, error.recv)

    def workIt(what: String)(mkClient: => (Service[HttpRequest, StreamResponse], SocketAddress)) {
      what in {
        val (client, address) = mkClient
        doAfter { client.close() }

        "writes from the server arrive on the client's channel" in {
          val clientRes = Await.result(client(httpRequest), 1.second)
          var result = ""
          val latch = new CountDownLatch(1)
          (clientRes.error?) ensure {
            Future { latch.countDown() }
          }

          clientRes.messages foreach { channelBuffer =>
            Future {
              result += channelBuffer.toString(Charset.defaultCharset)
            }
          }

          messages !! ChannelBuffers.wrappedBuffer("1".getBytes)
          messages !! ChannelBuffers.wrappedBuffer("2".getBytes)
          messages !! ChannelBuffers.wrappedBuffer("3".getBytes)
          error !! EOF

          latch.within(1.second)
          result mustEqual "123"
        }

        "writes from the server are queued before the client responds" in {
          val clientRes = Await.result(client(httpRequest), 1.second)
          messages !! ChannelBuffers.wrappedBuffer("1".getBytes)
          messages !! ChannelBuffers.wrappedBuffer("2".getBytes)
          messages !! ChannelBuffers.wrappedBuffer("3".getBytes)

          val latch = new CountDownLatch(3)
          var result = ""
          clientRes.messages foreach { channelBuffer =>
            Future {
              result += channelBuffer.toString(Charset.defaultCharset)
              latch.countDown()
            }
          }

          latch.within(1.second)
          error !! EOF
          result mustEqual "123"
        }

        "the client does not admit concurrent requests" in {
          val clientRes = Await.result(client(httpRequest), 1.second)
          client(httpRequest).poll must beLike {
            case Some(Throw(_: TooManyConcurrentRequestsException)) => true
          }
        }

        skip("the server does not admit concurrent requests")
        "the server does not admit concurrent requests" in {
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

          doAfter {
            bootstrap.releaseExternalResources()
          }

          val connectFuture = bootstrap
            .connect(address)
            .awaitUninterruptibly()
          connectFuture.isSuccess must beTrue
          val channel = connectFuture.getChannel

          doAfter {
            channel.close()
          }

          // first request is accepted
          channel
            .write(httpRequest)
            .awaitUninterruptibly()
            .isSuccess must beTrue

          messages !! ChannelBuffers.wrappedBuffer("chunk1".getBytes)

          Await.result(recvd?, 1.second) must beLike {
            case e: ChannelStateEvent =>
              e.getState == ChannelState.OPEN && (java.lang.Boolean.TRUE equals e.getValue)
          }

          Await.result(recvd?, 1.second) must beLike {
            case m: MessageEvent =>
              m.getMessage must beLike {
                case res: HttpResponse => res.isChunked
              }
          }

          Await.result(recvd?, 1.second) must beLike {
            case m: MessageEvent =>
              m.getMessage must beLike {
                case res: HttpChunk => !res.isLast  // get "chunk1"
              }
          }

          // The following requests should be ignored
          channel
            .write(httpRequest)
            .awaitUninterruptibly()
            .isSuccess must beTrue

          // the streaming should continue
          messages !! ChannelBuffers.wrappedBuffer("chunk2".getBytes)

          Await.result(recvd?, 1.second) must beLike {
            case m: MessageEvent =>
              m.getMessage must beLike {
                case res: HttpChunk => !res.isLast  // get "chunk2"
              }
          }

          error !! EOF
          Await.result(recvd?, 1.second) must beLike {
            case m: MessageEvent =>
              m.getMessage must beLike {
                case res: HttpChunkTrailer => res.isLast
              }
          }

          // And finally it's closed.
          Await.result(recvd?, 1.second) must beLike {
            case e: ChannelStateEvent =>
              e.getState == ChannelState.OPEN && (java.lang.Boolean.FALSE equals e.getValue)
          }
        }

        "server ignores channel buffer messages after channel close" in {
          val clientRes = Await.result(client(httpRequest), 1.second)
          var result = ""
          val latch = new CountDownLatch(1)

          (clientRes.error?) ensure {
            Future { latch.countDown() }
          }

          clientRes.messages foreach { channelBuffer =>
            Future {
              result += channelBuffer.toString(Charset.defaultCharset)
            }
          }

          FuturePool.unboundedPool {
            messages !! ChannelBuffers.wrappedBuffer("12".getBytes)
            messages !! ChannelBuffers.wrappedBuffer("23".getBytes)
            error !! EOF
            messages !! ChannelBuffers.wrappedBuffer("34".getBytes)
          }

          latch.within(1.second)
          result mustEqual "1223"
        }
      }
    }

    workIt("straight") {
      val server = ServerBuilder()
        .codec(new Stream)
        .bindTo(new InetSocketAddress(0))
        .name("Streams")
        .build(new MyService(serverRes))
      val address = server.localAddress
      val factory = ClientBuilder()
        .codec(new Stream)
        .hosts(Seq(address))
        .hostConnectionLimit(1)
        .buildFactory()

      val underlying = Await.result(factory())
      val client = new ServiceProxy[HttpRequest, StreamResponse](underlying) {
        override def close(deadline: Time) =
          Closable.all(underlying, server, factory).close(deadline)
      }

      (client, address)
    }

    workIt("proxy") {
      val server = ServerBuilder()
        .codec(new Stream)
        .bindTo(new InetSocketAddress(0))
        .name("streamserver")
        .build(new MyService(serverRes))

      val serverClient = ClientBuilder()
        .codec(new Stream)
        .hosts(Seq(server.localAddress))
        .hostConnectionLimit(1)
        .build()

      val proxy = ServerBuilder()
        .codec(new Stream)
        .bindTo(new InetSocketAddress(0))
        .name("streamproxy")
        .build(serverClient)

      val factory = ClientBuilder()
        .codec(new Stream)
        .hosts(Seq(proxy.localAddress))
        .hostConnectionLimit(1)
        .buildFactory()

      val underlying = Await.result(factory())
      val client = new ServiceProxy[HttpRequest, StreamResponse](underlying) {
        override def close(deadline: Time) =
          Closable.all(server, serverClient, proxy, factory).close(deadline)
      }

      (client, proxy.localAddress)
    }

    // TODO: investigate test flakiness (CSL-117)
    "delay release until complete response" in {
      if (false) {
        @volatile var count: Int = 0

        val server = ServerBuilder()
          .codec(new Stream)
          .bindTo(new InetSocketAddress(0))
          .name("Streams")
          .build((new MyService(serverRes)) map { r: HttpRequest =>
            synchronized { count += 1 }
            r
          })
        val client = ClientBuilder()
          .codec(new Stream)
          .hosts(Seq(server.localAddress))
          .hostConnectionLimit(1)
          .retries(2)
          .build()

        doAfter {
          client.close()
          server.close()
        }

        val res = Await.result(client(httpRequest), 1.second)
        count must be_==(1)
        val f2 = client(httpRequest)
        f2.poll must beNone  // because of the host connection limit

        messages !! ChannelBuffers.wrappedBuffer("1".getBytes)
        (res.messages??).toString(Charset.defaultCharset) must be_==("1")
        count must be_==(1)
        error !! EOF
        res.release()
        val res2 = Await.result(f2, 1.second)
        count must be_==(2)
        res2.release()
      }
    }
  }
}
