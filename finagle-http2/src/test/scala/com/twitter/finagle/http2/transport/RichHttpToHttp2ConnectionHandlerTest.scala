package com.twitter.finagle.http2.transport

import com.twitter.finagle.http2.transport.StreamMessage._
import io.netty.buffer.Unpooled
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.{ChannelHandlerContext, DefaultChannelPromise}
import io.netty.handler.codec.http.{
  DefaultFullHttpRequest,
  DefaultHttpContent,
  DefaultHttpRequest,
  DefaultLastHttpContent,
  HttpMethod,
  HttpVersion
}
import io.netty.handler.codec.http2.HttpConversionUtil.ExtensionHeaderNames
import io.netty.handler.codec.http2._
import org.mockito.ArgumentCaptor
import org.mockito.Matchers.{eq => meq, _}
import org.mockito.Mockito.{RETURNS_SMART_NULLS, verify, when}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite}

class RichHttpToHttp2ConnectionHandlerTest extends FunSuite with BeforeAndAfter with MockitoSugar {

  var mockCtx: ChannelHandlerContext = null
  var connectionHandler: HttpToHttp2ConnectionHandler = null
  var mockEncoder: Http2ConnectionEncoder = null

  before {
    val mockConnection = mock[DefaultHttp2Connection]
    mockEncoder = mock[Http2ConnectionEncoder](RETURNS_SMART_NULLS)
    val mockDecoder = mock[Http2ConnectionDecoder](RETURNS_SMART_NULLS)
    mockCtx = mock[ChannelHandlerContext](RETURNS_SMART_NULLS)

    val channel = new EmbeddedChannel()

    when(mockCtx.newPromise()).thenReturn(new DefaultChannelPromise(channel))
    when(mockEncoder.connection()).thenReturn(mockConnection)
    when(mockDecoder.connection()).thenReturn(mockConnection)

    val settings = new Http2Settings()

    connectionHandler =
      new RichHttpToHttp2ConnectionHandler(mockDecoder, mockEncoder, settings, () => ())
  }

  test("Sets default stream-dependency and weight") {
    val request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
    request.headers().add(ExtensionHeaderNames.SCHEME.text(), "https")

    val streamId = 1

    val p = mockCtx.newPromise()
    connectionHandler.write(mockCtx, Message(request, streamId), p)

    verify(mockEncoder).writeHeaders(
      meq(mockCtx),
      meq(streamId),
      anyObject(),
      meq(0),
      meq(true),
      meq(p)
    )
  }

  test("Allows client to specify stream-dependency-id and weight") {
    val request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
    request.headers().add(ExtensionHeaderNames.SCHEME.text(), "https")

    val streamDependencyId: Int = 15
    val weight = 4.toShort
    val streamId = 1

    request.headers().addInt(ExtensionHeaderNames.STREAM_DEPENDENCY_ID.text(), streamDependencyId)
    request.headers().addInt(ExtensionHeaderNames.STREAM_WEIGHT.text(), weight)

    val p = mockCtx.newPromise()
    connectionHandler.write(mockCtx, Message(request, streamId), p)

    verify(mockEncoder).writeHeaders(
      meq(mockCtx),
      meq(streamId),
      anyObject(),
      meq(0),
      meq(true),
      meq(p)
    )
  }

  test("Transmits full request w/o payload and w/o trailers") {
    val request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/")
    request.headers.add(ExtensionHeaderNames.SCHEME.text(), "https")
    request.headers.add("bar", "baz")

    val captor = ArgumentCaptor.forClass(classOf[Http2Headers])

    val p = mockCtx.newPromise()
    connectionHandler.write(mockCtx, Message(request, 1), p)

    verify(mockEncoder).writeHeaders(
      meq(mockCtx),
      meq(1),
      captor.capture(),
      meq(0),
      meq(true),
      meq(p)
    )

    assert(captor.getValue.get("bar") == "baz")
  }

  test("Transmits full request w/ payload but w/o trailers") {
    val payload = Unpooled.wrappedBuffer("foo".getBytes)
    val request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", payload)
    request.headers.add(ExtensionHeaderNames.SCHEME.text(), "https")
    request.headers.add("bar", "baz")

    val captor = ArgumentCaptor.forClass(classOf[Http2Headers])

    val p = mockCtx.newPromise()
    connectionHandler.write(mockCtx, Message(request, 1), p)

    verify(mockEncoder).writeHeaders(
      meq(mockCtx),
      meq(1),
      captor.capture(),
      meq(0),
      meq(false),
      meq(p)
    )

    assert(captor.getValue.get("bar") == "baz")

    verify(mockEncoder).writeData(
      meq(mockCtx),
      meq(1),
      meq(payload),
      meq(0),
      meq(true),
      meq(p)
    )
  }

  test("Transmits full request w/o payload but w/ trailers") {
    val request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/")
    request.headers.add(ExtensionHeaderNames.SCHEME.text(), "https")
    request.headers.add("bar", "baz")
    request.trailingHeaders.add("qux", "qwe")

    val headersCaptor = ArgumentCaptor.forClass(classOf[Http2Headers])
    val trailersCaptor = ArgumentCaptor.forClass(classOf[Http2Headers])

    val p = mockCtx.newPromise()
    connectionHandler.write(mockCtx, Message(request, 1), p)

    verify(mockEncoder).writeHeaders(
      meq(mockCtx),
      meq(1),
      headersCaptor.capture(),
      meq(0),
      meq(false),
      meq(p)
    )

    assert(headersCaptor.getValue.get("bar") == "baz")

    verify(mockEncoder).writeHeaders(
      meq(mockCtx),
      meq(1),
      trailersCaptor.capture(),
      meq(0),
      meq(true),
      meq(p)
    )

    assert(trailersCaptor.getValue.get("qux") == "qwe")
  }

  test("Transmits full request w/ payload and w/ trailers") {
    val payload = Unpooled.wrappedBuffer("foo".getBytes)
    val request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", payload)
    request.headers.add(ExtensionHeaderNames.SCHEME.text(), "https")
    request.headers.add("bar", "baz")
    request.trailingHeaders.add("qux", "qwe")

    val headersCaptor = ArgumentCaptor.forClass(classOf[Http2Headers])
    val trailersCaptor = ArgumentCaptor.forClass(classOf[Http2Headers])

    val p = mockCtx.newPromise()
    connectionHandler.write(mockCtx, Message(request, 1), p)

    verify(mockEncoder).writeHeaders(
      meq(mockCtx),
      meq(1),
      headersCaptor.capture(),
      meq(0),
      meq(false),
      meq(p)
    )

    assert(headersCaptor.getValue.get("bar") == "baz")

    verify(mockEncoder).writeData(
      meq(mockCtx),
      meq(1),
      meq(payload),
      meq(0),
      meq(false),
      meq(p)
    )

    verify(mockEncoder).writeHeaders(
      meq(mockCtx),
      meq(1),
      trailersCaptor.capture(),
      meq(0),
      meq(true),
      meq(p)
    )

    assert(trailersCaptor.getValue.get("qux") == "qwe")
  }

  test("Transmits request w/o payload") {
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/")
    request.headers.add(ExtensionHeaderNames.SCHEME.text(), "https")
    request.headers.add("bar", "baz")

    val captor = ArgumentCaptor.forClass(classOf[Http2Headers])

    val p = mockCtx.newPromise()
    connectionHandler.write(mockCtx, Message(request, 1), p)

    verify(mockEncoder).writeHeaders(
      meq(mockCtx),
      meq(1),
      captor.capture(),
      meq(0),
      meq(false),
      meq(p)
    )

    assert(captor.getValue.get("bar") == "baz")
  }

  test("Transmits payload and trailers in the last chunk") {
    val payload = Unpooled.wrappedBuffer("foo".getBytes)
    val chunk = new DefaultLastHttpContent(payload)
    chunk.trailingHeaders().add("bar", "baz")

    val captor = ArgumentCaptor.forClass(classOf[Http2Headers])

    val p = mockCtx.newPromise()
    connectionHandler.write(mockCtx, Message(chunk, 1), p)

    verify(mockEncoder).writeData(
      meq(mockCtx),
      meq(1),
      meq(payload),
      meq(0),
      meq(false),
      meq(p)
    )

    verify(mockEncoder).writeHeaders(
      meq(mockCtx),
      meq(1),
      captor.capture(),
      meq(0),
      meq(true),
      meq(p)
    )

    assert(captor.getValue.get("bar") == "baz")
  }

  test("Transmits payload in chunks") {
    val payload = Unpooled.wrappedBuffer("foo".getBytes)
    val chunk = new DefaultHttpContent(payload)

    val p = mockCtx.newPromise()
    connectionHandler.write(mockCtx, Message(chunk, 1), p)

    verify(mockEncoder).writeData(
      meq(mockCtx),
      meq(1),
      meq(payload),
      meq(0),
      meq(false),
      meq(p)
    )
  }
}
