package com.twitter.finagle.http2.transport

import com.twitter.concurrent.AsyncQueue
import com.twitter.conversions.time._
import com.twitter.finagle.FailureFlags
import com.twitter.finagle.http2.transport.Http2ClientDowngrader._
import com.twitter.finagle.http2.transport.MultiplexedTransporter._
import com.twitter.finagle.transport.QueueTransport
import com.twitter.util.{Await, Future}
import io.netty.handler.codec.http.LastHttpContent
import java.net.SocketAddress
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
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
}
