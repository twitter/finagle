package com.twitter.finagle.stream

import java.nio.charset.Charset
import java.util.concurrent.Executors

import org.jboss.netty.buffer.{ChannelBuffers, ChannelBuffer}
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.channel.{
  ChannelPipelineFactory, ChannelUpstreamHandler,
  ChannelHandlerContext, MessageEvent, ChannelEvent, Channels,
  ChannelStateEvent, ChannelState, WriteCompletionEvent}
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory

import org.specs.Specification


import com.twitter.concurrent._
import com.twitter.conversions.time._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.{SimpleFilter, Service, ServiceNotAvailableException, ClientCodecConfig}
import org.jboss.netty.util.CharsetUtil
import com.twitter.util._

object EndToEndSpec extends Specification {
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
    "work" in {
      val address = RandomSocket()
      val httpRequest = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
      val httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      val messages = new Broker[ChannelBuffer]
      val error = new Broker[Throwable]
      val serverRes = MyStreamResponse(httpResponse, messages.recv, error.recv)
      val server = ServerBuilder()
        .codec(new Stream)
        .bindTo(address)
        .name("Streams")
        .build(new MyService(serverRes))
      val clientFactory = ClientBuilder()
        .codec(new Stream)
        .hosts(Seq(address))
        .hostConnectionLimit(1)
        .buildFactory()
      val client = clientFactory.make()()

      doAfter {
        client.release()
        clientFactory.close()
        server.close()
      }

      "writes from the server arrive on the client's channel" in {
        val clientRes = client(httpRequest)(1.second)
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
        val clientRes = client(httpRequest)(1.second)
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
        val clientRes = client(httpRequest)(1.second)
        client(httpRequest).poll must beLike {
          case Some(Throw(_: ServiceNotAvailableException)) => true
        }
      }

      "the server does not admit concurrent requests" in {
        import com.twitter.finagle.util.Conversions._
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

        (recvd?)(1.second) must beLike {
          case e: ChannelStateEvent =>
            e.getState == ChannelState.OPEN && (java.lang.Boolean.TRUE equals e.getValue)
        }

        (recvd?)(1.second) must beLike {
          case m: MessageEvent =>
            m.getMessage must beLike {
              case res: HttpResponse => res.isChunked
            }
        }

        (recvd?)(1.second) must beLike {
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

        (recvd?)(1.second) must beLike {
          case m: MessageEvent =>
            m.getMessage must beLike {
              case res: HttpChunk => !res.isLast  // get "chunk2"
            }
        }

        error !! EOF
        (recvd?)(1.second) must beLike {
          case m: MessageEvent =>
            m.getMessage must beLike {
              case res: HttpChunkTrailer => res.isLast
            }
        }

        // And finally it's closed.
        (recvd?)(1.second) must beLike {
          case e: ChannelStateEvent =>
            e.getState == ChannelState.OPEN && (java.lang.Boolean.FALSE equals e.getValue)
        }
      }

      "server ignores channel buffer messages after channel close" in {
        val clientRes = client(httpRequest)(1.second)
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

        FuturePool.defaultPool {
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
}
