package com.twitter.finagle.http2.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.http2.transport.Http2ClientDowngrader._
import com.twitter.finagle.http2.transport.MultiplexedTransporter._
import com.twitter.finagle.liveness.FailureDetector
import com.twitter.finagle.transport.QueueTransport
import com.twitter.finagle.{FailureFlags, Status, StreamClosedException, Stack}
import com.twitter.util.{Await, Future, TimeoutException}
import io.netty.buffer._
import io.netty.handler.codec.http._
import io.netty.handler.codec.http2.Http2Error
import java.net.SocketAddress
import java.nio.charset.StandardCharsets
import org.scalatest.FunSuite

class MultiplexedTransporterTest extends FunSuite {

  class SlowClosingQueue(left: AsyncQueue[StreamMessage], right: AsyncQueue[StreamMessage])
      extends QueueTransport[StreamMessage, StreamMessage](left, right) {
    override val onClose: Future[Throwable] = Future.never
  }

  val H1Req = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "twitter.com")

  test("MultiplexedTransporter children should kill themselves when given a bad stream id") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq)
    val addr = new SocketAddress {}
    val multi = new MultiplexedTransporter(transport, addr, Stack.Params.empty)
    multi.setStreamId(Int.MaxValue)

    val first = multi().get
    val second = multi().get

    first.write(H1Req)
    second.write(H1Req)

    assert(!first.onClose.isDefined)
    assert(second.onClose.isDefined)
    val exn = intercept[StreamIdOverflowException] {
      throw Await.result(second.onClose, 5.seconds)
    }
    assert(exn.flags == FailureFlags.Retryable)
    assert(multi.numChildren == 1)
  }

  test("MultiplexedTransporter children should kill themselves when they grow to a bad stream id") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq)
    val addr = new SocketAddress {}
    val multi = new MultiplexedTransporter(transport, addr, Stack.Params.empty)
    multi.setStreamId(Int.MaxValue)

    val child = multi().get

    assert(!child.onClose.isDefined)
    child.write(H1Req)
    readq.offer(Message(LastHttpContent.EMPTY_LAST_CONTENT, Int.MaxValue))
    child.read()

    // Grow the bad ID
    child.write(H1Req)
    assert(child.onClose.isDefined)
    val exn = intercept[StreamIdOverflowException] {
      throw Await.result(child.onClose, 5.seconds)
    }
    assert(exn.flags == FailureFlags.Retryable)
    assert(multi.numChildren == 0)
  }

  test("MultiplexedTransporter children should disappear when they die") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq)
    val addr = new SocketAddress {}
    val multi = new MultiplexedTransporter(transport, addr, Stack.Params.empty)

    val conn = multi().get

    conn.write(H1Req)
    assert(multi.numChildren == 1)

    assert(!conn.onClose.isDefined)
    Await.result(conn.close(), 5.seconds)

    assert(multi.numChildren == 0)
  }

  test("MultiplexedTransporter children can't even") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq)
    val addr = new SocketAddress {}
    val multi = new MultiplexedTransporter(transport, addr, Stack.Params.empty)
    multi.setStreamId(2)

    val child = multi().get

    child.write(H1Req)

    assert(child.onClose.isDefined)
    val exn = intercept[IllegalStreamIdException] {
      throw Await.result(child.onClose, 5.seconds)
    }
    assert(exn.flags == FailureFlags.Retryable)
    assert(multi.numChildren == 0)
  }

  test("MultiplexedTransporter forbids new child streams on GOAWAY") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq)
    val addr = new SocketAddress {}
    val multi = new MultiplexedTransporter(transport, addr, Stack.Params.empty)

    val child = multi().get
    readq.offer(GoAway(LastHttpContent.EMPTY_LAST_CONTENT, 1, Http2Error.PROTOCOL_ERROR.code))

    intercept[DeadConnectionException] {
      multi().get
    }
    assert(multi.numChildren == 0)
  }

  test("MultiplexedTransporter respects last stream ID on GOAWAY & closes children") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq)
    val addr = new SocketAddress {}
    val multi = new MultiplexedTransporter(transport, addr, Stack.Params.empty)

    val c1, c3 = multi().get

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

    // Despite the GOAWAY, this child should be busy until it has been read
    assert(c1.status == Status.Open)
    assert(c3.status == Status.Closed)

    assert(Await.result(c1.read(), 5.seconds) == res)

    assert(c1.status == Status.Closed)

    // Despite the GOAWAY, this child should remain open until it has been read

    intercept[StreamClosedException] {
      Await.result(c3.read(), 5.seconds)
    }
    assert(multi.numChildren == 0)
  }

  test("MultiplexedTransporter reflects detector status") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq)
    val addr = new SocketAddress {}
    var cur: Status = Status.Open
    val params = Stack.Params.empty + FailureDetector.Param(
      new FailureDetector.MockConfig(() => cur)
    )
    val multi = new MultiplexedTransporter(transport, addr, params)

    assert(multi.status == Status.Open)
    cur = Status.Busy
    assert(multi.status == Status.Busy)
    assert(multi.numChildren == 0)
  }

  test("MultiplexedTransporter call to first() provides child with streamId == 1") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq)
    val addr = new SocketAddress {}
    val multi = new MultiplexedTransporter(transport, addr, Stack.Params.empty)
    assert(multi.first().asInstanceOf[multi.ChildTransport].curId == 1)
  }

  test("MultiplexedTransporter children increment stream ID only on write") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq)
    val addr = new SocketAddress {}
    val multi = new MultiplexedTransporter(transport, addr, Stack.Params.empty)

    val cA, cB = multi().get.asInstanceOf[multi.ChildTransport]

    cB.write(H1Req)
    cA.write(H1Req)

    // An attempt to hide implementation details.
    assert(cA.curId - cB.curId == 2)
    assert(multi.numChildren == 2)
  }

  test("Idle children kill themselves when read") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq)
    val addr = new SocketAddress {}
    val multi = new MultiplexedTransporter(transport, addr, Stack.Params.empty)

    val child = multi().get

    val thrown = intercept[BadChildStateException] {
      Await.result(child.read(), 5.seconds)
    }

    assert(thrown.isFlagged(FailureFlags.NonRetryable))
    assert(child.status == Status.Closed)
    assert(multi.numChildren == 0)
  }

  test("Children can be closed multiple times, but keep the first reason") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq)
    val addr = new SocketAddress {}
    val multi = new MultiplexedTransporter(transport, addr, Stack.Params.empty)

    val child = multi().get.asInstanceOf[multi.ChildTransport]

    val t1 = new Throwable("derp")
    val t2 = new Throwable("blam")

    child.closeWith(t1)
    child.closeWith(t2)

    val thrown = intercept[Throwable] {
      Await.result(child.read(), 5.seconds)
    }

    assert(thrown.getMessage.equals("derp"))
    assert(multi.numChildren == 0)
  }

  test("RST is sent when a message is received for a closed stream") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq)
    val addr = new SocketAddress {}
    var cur: Status = Status.Open
    val params = Stack.Params.empty + FailureDetector.Param(
      new FailureDetector.MockConfig(() => cur)
    )
    val multi = new MultiplexedTransporter(transport, addr, params)

    multi.setStreamId(5)

    readq.offer(Message(LastHttpContent.EMPTY_LAST_CONTENT, 3))

    val result = Await.result(writeq.poll(), 5.seconds)
    assert(result == Rst(3, Http2Error.STREAM_CLOSED.code))
    assert(multi.numChildren == 0)
  }

  test("GOAWAY w/ PROTOCOL_ERROR is sent when a message is received for an idle stream") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq)
    val addr = new SocketAddress {}
    var cur: Status = Status.Open
    val params = Stack.Params.empty + FailureDetector.Param(
      new FailureDetector.MockConfig(() => cur)
    )
    val multi = new MultiplexedTransporter(transport, addr, params)

    multi.setStreamId(5)

    readq.offer(Message(LastHttpContent.EMPTY_LAST_CONTENT, 11))

    val result = Await.result(writeq.poll(), 5.seconds)
    val GoAway(_, lastId, errorCode) = result
    assert(lastId == 5)
    assert(errorCode == Http2Error.PROTOCOL_ERROR.code)
    assert(multi.numChildren == 0)
  }

  test("RST is not sent when RST is received for a nonexistent stream") {
    val (writeq, readq) = (new AsyncQueue[StreamMessage](), new AsyncQueue[StreamMessage]())
    val transport = new SlowClosingQueue(writeq, readq)
    val addr = new SocketAddress {}
    var cur: Status = Status.Open
    val params = Stack.Params.empty + FailureDetector.Param(
      new FailureDetector.MockConfig(() => cur)
    )
    val multi = new MultiplexedTransporter(transport, addr, params)

    readq.offer(Rst(11, Http2Error.INTERNAL_ERROR.code))

    intercept[TimeoutException] {
      Await.result(writeq.poll(), 100.milliseconds)
    }
    assert(multi.numChildren == 0)
  }
}
