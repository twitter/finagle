package com.twitter.finagle.http2.transport.client

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.http.Request
import com.twitter.finagle.http2.param.{
  MaxConcurrentStreams,
  MaxRequestsPerSession
}
import com.twitter.finagle.netty4.http.Bijections
import com.twitter.finagle.{FailureFlags, Stack, Status}
import com.twitter.util.{Await, Awaitable}
import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.{Channel, ChannelHandler, ChannelInitializer}
import io.netty.handler.codec.http2.{
  Http2MultiplexCodec,
  Http2MultiplexCodecBuilder,
  Http2StreamFrameToHttpObjectCodec
}
import org.scalatest.funsuite.AnyFunSuite

class ClientSessionImplTest extends AnyFunSuite {

  abstract class Ctx {

    def await[T](t: Awaitable[T]): T = {
      testChannel.runPendingTasks()
      Await.result(t, 15.seconds)
    }

    def inboundInitializer: ChannelHandler = new ChannelInitializer[Channel] {
      def initChannel(ch: Channel): Unit =
        throw new IllegalStateException("Shouldn't get here.")
    }

    def failureDetectorStatus: () => Status = () => Status.Open

    def params: Stack.Params = Stack.Params.empty

    def initializer: ChannelInitializer[Channel] =
      new ChannelInitializer[Channel] {
        def initChannel(ch: Channel): Unit = {
          ch.pipeline
            .addLast(new Http2StreamFrameToHttpObjectCodec(false, false))
        }
      }

    lazy val multiplexCodec: Http2MultiplexCodec =
      Http2MultiplexCodecBuilder
        .forClient(inboundInitializer)
        .build()

    lazy val testChannel: EmbeddedChannel = {
      val ch = new EmbeddedChannel(multiplexCodec)
      ch
    }

    lazy val clientSession: ClientSession =
      new ClientSessionImpl(params, initializer, testChannel, failureDetectorStatus)
  }

  test("presents status as closed if the parent channel is closed") {
    new Ctx {
      assert(clientSession.status == Status.Open)

      testChannel.close()

      assert(clientSession.status == Status.Closed)
    }
  }

  test("Child streams present status as closed if the parent channel is closed") {
    new Ctx {
      val stream = await(clientSession.newChildTransport())
      assert(stream.status == Status.Open)

      testChannel.close()

      assert(stream.status == Status.Closed)
    }
  }

  test("No streams are initialized until the first write happens") {
    new Ctx {
      val stream = await(clientSession.newChildTransport())
      assert(stream.status == Status.Open)

      assert(multiplexCodec.connection.local.lastStreamCreated == 0)

      val req = Bijections.finagle.requestToNetty(Request(), None)
      await(stream.write(req))

      assert(multiplexCodec.connection.local.lastStreamCreated == 3)
    }
  }

  test("Session that has received a GOAWAY reports its status as Closed") {
    new Ctx {
      assert(clientSession.status == Status.Open)
      multiplexCodec.connection.goAwayReceived(0, 0, Unpooled.EMPTY_BUFFER)
      assert(clientSession.status == Status.Closed)
    }
  }

  test("Starting a dispatch after a GOAWAY results in a rejection") {
    new Ctx {
      multiplexCodec.connection.goAwayReceived(0, 0, Unpooled.EMPTY_BUFFER)
      assert(clientSession.status == Status.Closed)

      val ex = intercept[FailureFlags[_]] {
        await(clientSession.newChildTransport())
      }
      assert(ex.isFlagged(FailureFlags.Rejected))
      assert(ex.isFlagged(FailureFlags.Retryable))
    }
  }

  test("By default status is Closed when we're less than 50 streams away from exhausting the identifiers") {
    new Ctx {
      assert(clientSession.status == Status.Open)

      multiplexCodec.connection.local.createStream(Int.MaxValue - 100, false)
      assert(clientSession.status == Status.Open)

      // client streams are odd streams so to be less than 50 we need to multiply by 2.
      multiplexCodec.connection.local.createStream(Int.MaxValue - 100 + 2, false)
      assert(clientSession.status == Status.Closed)
    }
  }

  test("Status is busy when we have exhausted the max concurrent stream limit") {
    new Ctx {
      assert(clientSession.status == Status.Open)
      // client streams are odd streams
      multiplexCodec.connection.local.maxActiveStreams(1)
      assert(clientSession.status == Status.Open)
      multiplexCodec.connection.local.createStream(1, false)
      assert(clientSession.status == Status.Busy)
    }
  }

  test("Status is closed when we have exhausted the max requests per session limit") {
    new Ctx {
      override def params: Stack.Params = Stack.Params.empty
        .+(MaxConcurrentStreams(Option(128L))).+(
        MaxRequestsPerSession(Option(50_000L))
      )
      assert(clientSession.status == Status.Open)

      private val expectedHighWatermark: Int = 50_000 * 2 - 128 * 2
      multiplexCodec.connection.local.createStream(expectedHighWatermark - 1, false)
      assert(clientSession.status == Status.Open)

      multiplexCodec.connection.local.createStream(expectedHighWatermark + 1, false)
      assert(clientSession.status == Status.Closed)
    }
  }

  test("dispatching results in a rejection if we have exhausted the max concurrent stream limit") {
    new Ctx {
      assert(clientSession.status == Status.Open)
      // client streams are odd streams
      multiplexCodec.connection.local.maxActiveStreams(1)
      multiplexCodec.connection.local.createStream(1, false)
      assert(clientSession.status == Status.Busy)

      val ex = intercept[FailureFlags[_]] {
        await(clientSession.newChildTransport())
      }
      assert(ex.isFlagged(FailureFlags.Rejected))
      assert(ex.isFlagged(FailureFlags.Retryable))
    }
  }

  test("Status is Closed if PingDetectionHandler is Closed") {
    new Ctx {
      var status: Status = Status.Open
      override def failureDetectorStatus = () => status
      assert(clientSession.status == Status.Open)
      status = Status.Closed
      assert(clientSession.status == Status.Closed)
    }
  }
}
