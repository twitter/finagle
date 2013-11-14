package com.twitter.finagle.http.codec

import com.twitter.finagle.http.Response
import com.twitter.io.Reader
import java.net.SocketAddress
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.{ChannelFuture, ChannelHandlerContext, Channel, MessageEvent}
import org.jboss.netty.handler.codec.http.HttpChunk
import org.junit.runner.RunWith
import org.mockito.Mockito.{stub, verify}
import org.mockito.Matchers.any
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.collection.mutable.Stack

@RunWith(classOf[JUnitRunner])
class ResponseEncoderTest extends FunSuite with MockitoSugar {

  test("set content-length when response is not chunked") {
    val payload = "h∑llø".getBytes("UTF-8")
    val response = Response()
    response.write(ChannelBuffers.wrappedBuffer(payload))

    val event = mock[MessageEvent]
    stub(event.getMessage()).toReturn(response)

    val encoder = new ResponseEncoder
    encoder.writeRequested(mock[ChannelHandlerContext], event)

    assert(response.contentLength.isDefined)
    assert(response.contentLength === Some(payload.length))
  }

  test("write chunks when response is chunked") {
    val downstream = new Stack[Object]
    val chunks = Seq("h∑llø".getBytes("UTF-8"),
                     "wørld".getBytes("UTF-8"),
                     Array[Byte](0.toByte, 65.toByte, 32.toByte, 65.toByte)) map { c =>
                       ChannelBuffers.wrappedBuffer(c) }

    val response = Response()
    response.setChunked(true)

    val event = mock[MessageEvent]
    stub(event.getMessage()).toReturn(response)
    stub(event.getRemoteAddress()).toReturn(mock[SocketAddress])
    stub(event.getFuture()).toReturn(mock[ChannelFuture])

    val ctx = mock[ChannelHandlerContext]
    stub(ctx.getChannel).toReturn(mock[Channel])

    val encoder = new ResponseEncoder {
      override def channelsWrite(ctx: ChannelHandlerContext, writeFuture: ChannelFuture,
        message: Object, remoteAddr: SocketAddress) {
        message match {
          case chunk: HttpChunk if !chunk.isLast => downstream.push(chunk.getContent)
          case _ => downstream.push(message)
        }
        writeFuture.setSuccess()
      }
    }

    assert(downstream.headOption === None)

    encoder.writeRequested(ctx, event)
    assert(downstream.headOption === Some(response))

    response.write(chunks(0))
    assert(downstream.headOption === Some(chunks(0)))

    response.write(chunks(1))
    assert(downstream.headOption === Some(chunks(1)))

    response.write(chunks(2))
    assert(downstream.headOption === Some(chunks(2)))

    response.close()
    assert(downstream.headOption === Some(HttpChunk.LAST_CHUNK))
  }

  test("propagate errors to producer") {
    val ctx = mock[ChannelHandlerContext]
    stub(ctx.getChannel).toReturn(mock[Channel])

    val response = Response()
    response.setChunked(true)

    val event = mock[MessageEvent]
    stub(event.getMessage()).toReturn(response)
    stub(event.getRemoteAddress()).toReturn(mock[SocketAddress])
    stub(event.getFuture()).toReturn(mock[ChannelFuture])

    val encoder = new ResponseEncoder {
      override def channelsWrite(ctx: ChannelHandlerContext, writeFuture: ChannelFuture,
        message: Object, remoteAddr: SocketAddress) {
        writeFuture.setFailure(new Exception)
      }
    }

    encoder.writeRequested(ctx, event)
    intercept[Reader.ReaderDiscarded] { response.write("a") }
  }

  test("reader failure") {
    val ctx = mock[ChannelHandlerContext]
    stub(ctx.getChannel).toReturn(mock[Channel])

    val response = Response()
    response.setChunked(true)

    val future = mock[ChannelFuture]

    val event = mock[MessageEvent]
    stub(event.getMessage()).toReturn(response)
    stub(event.getRemoteAddress()).toReturn(mock[SocketAddress])
    stub(event.getFuture()).toReturn(future)

    val encoder = new ResponseEncoder {
      override def channelsWrite(ctx: ChannelHandlerContext, writeFuture: ChannelFuture,
        message: Object, remoteAddr: SocketAddress) {
        writeFuture.setSuccess()
      }
    }

    encoder.writeRequested(ctx, event)
    response.reader.discard()
    verify(future).setFailure(any[Throwable])
  }
}
