package com.twitter.finagle.http2.transport

import com.twitter.finagle.http2.transport.Http2ClientDowngrader.Message
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.{DefaultChannelPromise, ChannelHandlerContext}
import io.netty.handler.codec.http.{
  DefaultFullHttpRequest,
  HttpMethod,
  HttpVersion
}
import io.netty.handler.codec.http2.HttpConversionUtil.ExtensionHeaderNames
import io.netty.handler.codec.http2._
import org.junit.runner.RunWith
import org.mockito.Matchers.{eq => meq, _}
import org.mockito.Mockito.{verify, when, RETURNS_SMART_NULLS}
import org.scalatest.mock.MockitoSugar
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner
@RunWith(classOf[JUnitRunner])
class RichHttpToHttp2ConnectionHandlerTest extends FunSuite with BeforeAndAfter with MockitoSugar {

  var mockCtx: ChannelHandlerContext = null
  var promise: DefaultChannelPromise = null
  var connectionHandler: HttpToHttp2ConnectionHandler = null
  var request: DefaultFullHttpRequest = null
  var mockEncoder: Http2ConnectionEncoder = null

  before {
    val mockConnection = mock[DefaultHttp2Connection]
    mockEncoder = mock[Http2ConnectionEncoder](RETURNS_SMART_NULLS)
    val mockDecoder = mock[Http2ConnectionDecoder](RETURNS_SMART_NULLS)
    mockCtx = mock[ChannelHandlerContext](RETURNS_SMART_NULLS)

    val channel = new EmbeddedChannel()

    promise = new DefaultChannelPromise(channel)

    when(mockCtx.newPromise()).thenReturn(promise)
    when(mockEncoder.connection()).thenReturn(mockConnection)
    when(mockDecoder.connection()).thenReturn(mockConnection)

    val settings = new Http2Settings()

    connectionHandler =
      new RichHttpToHttp2ConnectionHandler(mockDecoder, mockEncoder, settings, () => ())

    request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
    request.headers().add(ExtensionHeaderNames.SCHEME.text(), "https")
  }

  test("Client sets default stream-dependency and weight") {
    val streamId: Int = 1
    val defaultStreamDependency = 0
    val defaultWeight = Http2CodecUtil.DEFAULT_PRIORITY_WEIGHT

    val message = Message(request, 1)

    connectionHandler.write(mockCtx, message, promise)
    verify(mockEncoder).writeHeaders(
      meq(mockCtx),
      meq(streamId),
      anyObject(),
      meq(defaultStreamDependency),
      meq(defaultWeight),
      meq(false),
      meq(0),
      meq(true),
      meq(promise)
    )
  }

  test("Allows client to specify stream-dependency-id and weight") {
    val streamDependencyId: Int = 15
    val weight: Short = 4
    val streamId: Int = 1

    request.headers().addInt(ExtensionHeaderNames.STREAM_DEPENDENCY_ID.text(), streamDependencyId)
    request.headers().addInt(ExtensionHeaderNames.STREAM_WEIGHT.text(), weight)

    val message = Message(request, 1)

    connectionHandler.write(mockCtx, message, promise)
    verify(mockEncoder).writeHeaders(
      meq(mockCtx),
      meq(streamId),
      anyObject(),
      meq(streamDependencyId),
      meq(weight),
      meq(false),
      meq(0),
      meq(true),
      meq(promise)
    )
  }
}
