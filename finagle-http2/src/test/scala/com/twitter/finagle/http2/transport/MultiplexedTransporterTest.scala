package com.twitter.finagle.http2.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.http2.transport.Http2ClientDowngrader._
import com.twitter.finagle.http2.transport.MultiplexedTransporter._
import com.twitter.finagle.transport.QueueTransport
import com.twitter.finagle.{FailureFlags, Status, StreamClosedException}
import com.twitter.util.{Await, Future}
import io.netty.buffer._
import io.netty.handler.codec.http._
import java.net.SocketAddress
import java.nio.charset.StandardCharsets
import org.scalatest.FunSuite

class MultiplexedTransporterTest extends FunSuite {

  class SlowClosingQueue(
      left: AsyncQueue[StreamMessage],
      right: AsyncQueue[StreamMessage])
    extends QueueTransport[StreamMessage, StreamMessage](left, right) {
    override val onClose: Future[Throwable] = Future.never
  }

  test("MultiplexedTransporter children should kill themselves when born with a bad stream id") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq)
    val addr = new SocketAddress {}
    val multi = new MultiplexedTransporter(transport, addr)
    multi.setStreamId(Int.MaxValue)

    val first = multi().get
    val second = multi().get

    assert(!first.onClose.isDefined)
    assert(second.onClose.isDefined)
    val exn = intercept[StreamIdOverflowException] {
      throw Await.result(second.onClose, 5.seconds)
    }
    assert(exn.flags == FailureFlags.Retryable)
  }

  test("MultiplexedTransporter children should kill themselves when they grow to a bad stream id") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq)
    val addr = new SocketAddress {}
    val multi = new MultiplexedTransporter(transport, addr)
    multi.setStreamId(Int.MaxValue)

    val child = multi().get

    assert(!child.onClose.isDefined)
    child.write(LastHttpContent.EMPTY_LAST_CONTENT)
    readq.offer(Message(LastHttpContent.EMPTY_LAST_CONTENT, Int.MaxValue))
    child.read()

    assert(child.onClose.isDefined)
    val exn = intercept[StreamIdOverflowException] {
      throw Await.result(child.onClose, 5.seconds)
    }
    assert(exn.flags == FailureFlags.Retryable)
  }

  test("MultiplexedTransporter children can't even") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq)
    val addr = new SocketAddress {}
    val multi = new MultiplexedTransporter(transport, addr)
    multi.setStreamId(2)

    val child = multi().get

    assert(child.onClose.isDefined)
    val exn = intercept[IllegalStreamIdException] {
      throw Await.result(child.onClose, 5.seconds)
    }
    assert(exn.flags == FailureFlags.Retryable)
  }

  test("MultiplexedTransporter forbids new child streams on GOAWAY") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq)
    val addr = new SocketAddress {}
    val multi = new MultiplexedTransporter(transport, addr)

    val child = multi().get
    readq.offer(GoAway(LastHttpContent.EMPTY_LAST_CONTENT, 1))

    intercept[DeadConnectionException] {
      multi().get
    }
  }

  test("MultiplexedTransporter respects last stream ID on GOAWAY & closes children") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq)
    val addr = new SocketAddress {}
    val multi = new MultiplexedTransporter(transport, addr)

    val c1, c3 = multi().get

    val req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "twitter.com")
    val res = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.copiedBuffer("whatever", StandardCharsets.UTF_8))

    // Both streams make a request
    c1.write(req)
    c3.write(req)

    // Serve one response and GOAWAY
    readq.offer(Message(res, 1))
    readq.offer(GoAway(LastHttpContent.EMPTY_LAST_CONTENT, 1))

    assert(Await.result(c1.read(), 5.seconds) == res)

    intercept[StreamClosedException] {
      Await.result(c3.read(), 5.seconds)
    }

    assert(c1.status == Status.Closed)
    assert(c3.status == Status.Closed)
  }
}
