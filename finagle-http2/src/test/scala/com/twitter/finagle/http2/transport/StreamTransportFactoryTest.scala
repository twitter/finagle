package com.twitter.finagle.http2.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.http.TooLongMessageException
import com.twitter.finagle.http2.SerialExecutor
import com.twitter.finagle.http2.transport.Http2ClientDowngrader._
import com.twitter.finagle.http2.transport.StreamTransportFactory._
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle.netty4.transport.HasExecutor
import com.twitter.finagle.transport.{LegacyContext, QueueTransport, Transport, TransportContext}
import com.twitter.finagle.{FailureFlags, Stack, Status, StreamClosedException}
import com.twitter.util.{Await, Awaitable, Future, TimeoutException}
import io.netty.buffer._
import io.netty.handler.codec.http._
import io.netty.handler.codec.http2.Http2Exception.{HeaderListSizeException, headerListSizeError}
import io.netty.handler.codec.http2.Http2Error
import java.net.SocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executor
import org.scalatest.FunSuite

class StreamTransportFactoryTest extends FunSuite {

  def await[T](a: Awaitable[T]) = Await.result(a, 5.seconds)
  def ready[T](a: Awaitable[T]) = Await.ready(a, 5.seconds)

  class SlowClosingQueue(left: AsyncQueue[StreamMessage], right: AsyncQueue[StreamMessage])
      extends QueueTransport[StreamMessage, StreamMessage](left, right) {
    override val onClose: Future[Throwable] = Future.never
    override val context: TransportContext = new LegacyContext(this) with HasExecutor {
      private[finagle] override val executor: Executor = new SerialExecutor
    }
  }

  val H1Req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "twitter.com")

  test("StreamTransportFactory streams should kill themselves when given a bad stream id") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq).asInstanceOf[Transport[StreamMessage, StreamMessage] {
      type Context = TransportContext with HasExecutor
    }]
    val addr = new SocketAddress {}
    val streamFac = new StreamTransportFactory(transport, addr, Stack.Params.empty)
    streamFac.setStreamId(Int.MaxValue)

    val first = await(streamFac())
    val second = await(streamFac())

    first.write(H1Req)
    second.write(H1Req)

    assert(!first.onClose.isDefined)
    assert(second.onClose.isDefined)
    val exn = intercept[StreamIdOverflowException] {
      throw await(second.onClose)
    }
    assert(exn.flags == FailureFlags.Retryable)
    assert(streamFac.numActiveStreams == 1)
  }

  test("StreamTransportFactory streams should kill themselves when they grow to a bad stream id") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq).asInstanceOf[Transport[StreamMessage, StreamMessage] {
      type Context = TransportContext with HasExecutor
    }]
    val addr = new SocketAddress {}
    val streamFac = new StreamTransportFactory(transport, addr, Stack.Params.empty)
    streamFac.setStreamId(Int.MaxValue)

    val stream = await(streamFac())

    assert(!stream.onClose.isDefined)
    stream.write(H1Req)
    readq.offer(Message(LastHttpContent.EMPTY_LAST_CONTENT, Int.MaxValue))
    stream.read()

    // Grow the bad ID
    stream.write(H1Req)
    assert(stream.onClose.isDefined)
    val exn = intercept[StreamIdOverflowException] {
      throw await(stream.onClose)
    }
    assert(exn.flags == FailureFlags.Retryable)
    assert(streamFac.numActiveStreams == 0)
  }

  test("StreamTransportFactory streams should disappear when they die") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq).asInstanceOf[Transport[StreamMessage, StreamMessage] {
      type Context = TransportContext with HasExecutor
    }]
    val addr = new SocketAddress {}
    val streamFac = new StreamTransportFactory(transport, addr, Stack.Params.empty)

    val conn = await(streamFac())

    conn.write(H1Req)
    assert(streamFac.numActiveStreams == 1)

    assert(!conn.onClose.isDefined)
    await(conn.close())

    assert(streamFac.numActiveStreams == 0)
  }

  test("StreamTransportFactory streams can't even") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq).asInstanceOf[Transport[StreamMessage, StreamMessage] {
      type Context = TransportContext with HasExecutor
    }]
    val addr = new SocketAddress {}
    val streamFac = new StreamTransportFactory(transport, addr, Stack.Params.empty)
    streamFac.setStreamId(2)

    val stream = await(streamFac())

    stream.write(H1Req)

    assert(stream.onClose.isDefined)
    val exn = intercept[IllegalStreamIdException] {
      throw await(stream.onClose)
    }
    assert(exn.flags == FailureFlags.Retryable)
    assert(streamFac.numActiveStreams == 0)
  }

  test("StreamTransportFactory forbids new streams on GOAWAY") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq).asInstanceOf[Transport[StreamMessage, StreamMessage] {
      type Context = TransportContext with HasExecutor
    }]
    val addr = new SocketAddress {}
    val streamFac = new StreamTransportFactory(transport, addr, Stack.Params.empty)

    val stream = await(streamFac())
    readq.offer(GoAway(LastHttpContent.EMPTY_LAST_CONTENT, 1, Http2Error.PROTOCOL_ERROR.code))

    intercept[DeadConnectionException] {
      await(streamFac())
    }
    assert(streamFac.numActiveStreams == 0)
  }

  test("StreamTransportFactory respects last stream ID on GOAWAY & closes streams") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq).asInstanceOf[Transport[StreamMessage, StreamMessage] {
      type Context = TransportContext with HasExecutor
    }]
    val addr = new SocketAddress {}
    val streamFac = new StreamTransportFactory(transport, addr, Stack.Params.empty)

    val c1, c3 = await(streamFac())

    val res = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      HttpResponseStatus.OK,
      Unpooled.copiedBuffer("whatever", StandardCharsets.UTF_8)
    )

    // Both streams make a request
    c1.write(H1Req)
    c3.write(H1Req)

    // Serve one response and GOAWAY
    readq.offer(Message(res, 1))
    readq.offer(GoAway(LastHttpContent.EMPTY_LAST_CONTENT, 1, Http2Error.PROTOCOL_ERROR.code))

    // Despite the GOAWAY, this stream should be busy until it has been read
    assert(c1.status == Status.Open)
    assert(c3.status == Status.Closed)

    assert(await(c1.read()) == res)

    assert(c1.status == Status.Closed)

    // Despite the GOAWAY, this stream should remain open until it has been read

    intercept[StreamClosedException] {
      await(c3.read())
    }
    assert(streamFac.numActiveStreams == 0)
  }

  test("StreamTransportFactory reflects detector status") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq).asInstanceOf[Transport[StreamMessage, StreamMessage] {
      type Context = TransportContext with HasExecutor
    }]
    val addr = new SocketAddress {}
    var cur: Status = Status.Open
    val params = Stack.Params.empty + FailureDetector.Param(
      new FailureDetector.MockConfig(() => cur)
    )
    val streamFac = new StreamTransportFactory(transport, addr, params)

    assert(streamFac.status == Status.Open)
    cur = Status.Busy
    assert(streamFac.status == Status.Busy)
    assert(streamFac.numActiveStreams == 0)
  }

  test("StreamTransportFactory call to first() provides stream with streamId == 1") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq).asInstanceOf[Transport[StreamMessage, StreamMessage] {
      type Context = TransportContext with HasExecutor
    }]
    val addr = new SocketAddress {}
    val streamFac = new StreamTransportFactory(transport, addr, Stack.Params.empty)
    assert(streamFac.first().asInstanceOf[streamFac.StreamTransport].curId == 1)
  }

  test("StreamTransportFactory streams increment stream ID only on write") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq).asInstanceOf[Transport[StreamMessage, StreamMessage] {
      type Context = TransportContext with HasExecutor
    }]
    val addr = new SocketAddress {}
    val streamFac = new StreamTransportFactory(transport, addr, Stack.Params.empty)

    val cA, cB = await(streamFac()).asInstanceOf[streamFac.StreamTransport]

    cB.write(H1Req)
    cA.write(H1Req)

    // An attempt to hide implementation details.
    assert(cA.curId - cB.curId == 2)
    assert(streamFac.numActiveStreams == 2)
  }

  test("Idle streams kill themselves when read") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq).asInstanceOf[Transport[StreamMessage, StreamMessage] {
      type Context = TransportContext with HasExecutor
    }]
    val addr = new SocketAddress {}
    val streamFac = new StreamTransportFactory(transport, addr, Stack.Params.empty)

    val stream = await(streamFac())

    val thrown = intercept[BadStreamStateException] {
      await(stream.read())
    }

    assert(thrown.isFlagged(FailureFlags.NonRetryable))
    assert(stream.status == Status.Closed)
    assert(streamFac.numActiveStreams == 0)
  }

  test("Children can be closed streamFacple times, but keep the first reason") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq).asInstanceOf[Transport[StreamMessage, StreamMessage] {
      type Context = TransportContext with HasExecutor
    }]
    val addr = new SocketAddress {}
    val streamFac = new StreamTransportFactory(transport, addr, Stack.Params.empty)

    val stream = await(streamFac()).asInstanceOf[streamFac.StreamTransport]

    val t1 = new Throwable("derp")
    val t2 = new Throwable("blam")

    stream.handleCloseWith(t1)
    stream.handleCloseWith(t2)

    val thrown = intercept[Throwable] {
      await(stream.read())
    }

    assert(thrown.getMessage.equals("derp"))
    assert(streamFac.numActiveStreams == 0)
  }

  test("RST is sent when a message is received for a closed stream") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq).asInstanceOf[Transport[StreamMessage, StreamMessage] {
      type Context = TransportContext with HasExecutor
    }]
    val addr = new SocketAddress {}
    var cur: Status = Status.Open
    val params = Stack.Params.empty + FailureDetector.Param(
      new FailureDetector.MockConfig(() => cur)
    )
    val streamFac = new StreamTransportFactory(transport, addr, params)

    streamFac.setStreamId(5)

    readq.offer(Message(LastHttpContent.EMPTY_LAST_CONTENT, 3))

    val result = await(writeq.poll())
    assert(result == Rst(3, Http2Error.STREAM_CLOSED.code))
    assert(streamFac.numActiveStreams == 0)
  }

  test("GOAWAY w/ PROTOCOL_ERROR is sent when a message is received for an idle stream") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq).asInstanceOf[Transport[StreamMessage, StreamMessage] {
      type Context = TransportContext with HasExecutor
    }]
    val addr = new SocketAddress {}
    var cur: Status = Status.Open
    val params = Stack.Params.empty + FailureDetector.Param(
      new FailureDetector.MockConfig(() => cur)
    )
    val streamFac = new StreamTransportFactory(transport, addr, params)

    streamFac.setStreamId(5)

    readq.offer(Message(LastHttpContent.EMPTY_LAST_CONTENT, 11))

    val result = await(writeq.poll())
    val GoAway(_, lastId, errorCode) = result
    assert(lastId == 5)
    assert(errorCode == Http2Error.PROTOCOL_ERROR.code)
    assert(streamFac.numActiveStreams == 0)
  }

  test("RST is not sent when RST is received for a nonexistent stream") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq).asInstanceOf[Transport[StreamMessage, StreamMessage] {
      type Context = TransportContext with HasExecutor
    }]
    val addr = new SocketAddress {}
    var cur: Status = Status.Open
    val params = Stack.Params.empty + FailureDetector.Param(
      new FailureDetector.MockConfig(() => cur)
    )
    val streamFac = new StreamTransportFactory(transport, addr, params)

    readq.offer(Rst(11, Http2Error.INTERNAL_ERROR.code))

    intercept[TimeoutException] {
      await(writeq.poll())
    }
    assert(streamFac.numActiveStreams == 0)
  }

  test("RST is not sent when RST is received for an existent stream") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq).asInstanceOf[Transport[StreamMessage, StreamMessage] {
      type Context = TransportContext with HasExecutor
    }]
    val addr = new SocketAddress {}
    var cur: Status = Status.Open
    val params = Stack.Params.empty + FailureDetector.Param(
      new FailureDetector.MockConfig(() => cur)
    )
    val streamFac = new StreamTransportFactory(transport, addr, params)
    val conn = await(streamFac())

    streamFac.setStreamId(11)
    conn.write(H1Req)

    val req = await(writeq.poll())
    assert(req == Message(H1Req, 11))

    readq.offer(Rst(11, Http2Error.INTERNAL_ERROR.code))

    intercept[TimeoutException] {
      await(writeq.poll())
    }
    assert(streamFac.numActiveStreams == 0)
  }

  test("PINGs receive replies every time") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq).asInstanceOf[Transport[StreamMessage, StreamMessage] {
      type Context = TransportContext with HasExecutor
    }]
    val addr = new SocketAddress {}
    var cur: Status = Status.Open
    val params = Stack.Params.empty + FailureDetector.Param(
      new FailureDetector.MockConfig(() => cur)
    )
    val streamFac = new StreamTransportFactory(transport, addr, params)

    for (_ <- 1 to 10) {
      streamFac.ping()
      assert(await(writeq.poll()) == Ping)
      readq.offer(Ping)
    }
  }

  test("reading a StreamException fails that stream") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq).asInstanceOf[Transport[StreamMessage, StreamMessage] {
      type Context = TransportContext with HasExecutor
    }]

    val error = Http2Error.PROTOCOL_ERROR
    val hlsExn = headerListSizeError(3, error, true /* onDecode */, "too big") match {
      case e: HeaderListSizeException => e
      case _ => fail
    }

    val exn = StreamException(hlsExn, 3)

    val addr = new SocketAddress {}
    val streamFac = new StreamTransportFactory(transport, addr, Stack.Params.empty)
    streamFac.setStreamId(3)

    val stream = await(streamFac())
    stream.write(H1Req)
    readq.offer(exn)
    val f = stream.read()
    intercept[TooLongMessageException] {
      await(f)
    }
  }

  test("stream transports enter Dead state if the StreamTransportFactory loses track of them") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq).asInstanceOf[Transport[StreamMessage, StreamMessage] {
      type Context = TransportContext with HasExecutor
    }]
    val addr = new SocketAddress {}

    val streamFac = new StreamTransportFactory(transport, addr, Stack.Params.empty)

    val stream = streamFac.first().asInstanceOf[streamFac.StreamTransport]
    readq.offer(Message(LastHttpContent.EMPTY_LAST_CONTENT, 1))

    // fake a lost stream
    streamFac.removeStream(1)
    ready(stream.read())
    assert(stream.state == Dead)
  }
}
