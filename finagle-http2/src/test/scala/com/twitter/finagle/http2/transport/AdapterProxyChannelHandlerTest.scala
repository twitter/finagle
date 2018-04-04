package com.twitter.finagle.http2.transport

import com.twitter.finagle.Stack.Params
import com.twitter.finagle.http2.transport.Http2ClientDowngrader.{Message, Rst, StreamException}
import com.twitter.finagle.netty4.http
import com.twitter.finagle.stats.InMemoryStatsReceiver
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel._
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.http._
import io.netty.handler.codec.http2.Http2Exception.{HeaderListSizeException, headerListSizeError}
import io.netty.handler.codec.http2.Http2Error
import io.netty.util.concurrent.PromiseCombiner
import java.nio.charset.StandardCharsets
import org.scalatest.FunSuite

class AdapterProxyChannelHandlerTest extends FunSuite {
  def moon = Unpooled.copiedBuffer("goodnight moon", StandardCharsets.UTF_8)
  def stars = Unpooled.copiedBuffer("goodnight stars", StandardCharsets.UTF_8)
  def chunk() = Unpooled.wrappedBuffer("ref-counting on the jvm is a great idea".getBytes(StandardCharsets.UTF_8))
  def fullRequest = new DefaultFullHttpRequest(
    HttpVersion.HTTP_1_1,
    HttpMethod.GET,
    "twitter.com",
    moon
  )
  def fullResponse = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, moon)
  def request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "twitter.com")
  def dataReq = new DefaultHttpContent(moon)
  def starsReq = new DefaultHttpContent(stars)
  def messageReq = Message(fullRequest, 3)
  def messageRep = Message(fullResponse, 3)

  def validateObject(
    msg: Message,
    streamId: Int,
    isRequest: Boolean,
    content: Option[ByteBuf]
  ): Boolean = {
    val Message(obj, id) = msg
    if (id != streamId) return false
    obj match {
      case _: HttpRequest if !isRequest => return false
      case _: HttpResponse if isRequest => return false
      case _ =>
    }
    obj match {
      case _: HttpContent if !content.isDefined => false
      case data: HttpContent => data.content == content.get
      case _ => true
    }
  }

  test("writes things through") {
    val handler = new AdapterProxyChannelHandler(http.initClient(Params.empty))
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(handler)
    channel.writeOutbound(messageReq)
    assert(channel.readOutbound[Message]() == messageReq)
  }

  test("reads things through") {
    val handler = new AdapterProxyChannelHandler(http.initClient(Params.empty))
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(handler)
    channel.writeInbound(messageReq)
    val read = channel.readInbound[Message]()
    assert(read.equals(messageReq))
  }

  test("disaggregates reads") {
    val handler = new AdapterProxyChannelHandler({ pipeline =>
      pipeline.addLast(new Disaggregator())
    })
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(handler)
    channel.writeInbound(messageReq)

    assert(validateObject(channel.readInbound[Message](), 3, true, None))
    assert(validateObject(channel.readInbound[Message](), 3, false, Some(moon)))
  }

  test("disaggregates writes") {
    val handler = new AdapterProxyChannelHandler({ pipeline =>
      pipeline.addLast(new Disaggregator())
    })
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(handler)
    channel.writeOutbound(messageReq)

    assert(validateObject(channel.readOutbound[Message](), 3, true, None))
    assert(validateObject(channel.readOutbound[Message](), 3, false, Some(moon)))
  }

  test("aggregates writes") {
    val handler = new AdapterProxyChannelHandler({ pipeline =>
      pipeline.addLast(new Aggregator())
    })
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(handler)

    channel.writeOutbound(Message(request, 3))
    channel.writeOutbound(Message(dataReq, 3))
    assert(validateObject(channel.readOutbound[Message](), 3, true, Some(moon)))
  }

  test("cleans up pipelines for different streams") {
    val handler = new AdapterProxyChannelHandler({ pipeline =>
      http.initClient(Params.empty)(pipeline)
    })
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(handler)
    val content3 = chunk() // destined for stream #3
    assert(content3.refCnt() == 1)
    val content5 = chunk() // destined for stream #5
    assert(content5.refCnt() == 1)

    channel.writeOutbound(Message(request, 3))
    channel.writeOutbound(Message(request, 5))

    channel.writeOutbound(Message(dataReq, 3))
    assert(validateObject(channel.readOutbound[Message](), 3, true, Some(moon)))

    // both streams see inbound responses + data
    channel.writeInbound(Message(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK), 3))
    channel.writeInbound(Message(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK), 5))
    channel.writeInbound(Message(new DefaultHttpContent(content3), 3))
    channel.writeInbound(Message(new DefaultHttpContent(content5), 5))

    assert(content3.refCnt() == 1)
    assert(content5.refCnt() == 1)
    channel.writeInbound(Rst(3, 0x8)) // stream #3 sees a RST
    assert(content3.refCnt() == 0) // buffered data on #3 is cleaned up
    assert(content5.refCnt() == 1) // buffered data on stream #5 is unaffected

    channel.writeOutbound(Message(starsReq, 5))
    assert(validateObject(channel.readOutbound[Message](), 5, true, Some(stars)))

    // stream #5 sees the last message and its buffered data is released
    channel.writeInbound(Message(new DefaultLastHttpContent(), 5))
    assert(content5.refCnt() == 0)
  }

  test("different handlers for different streams") {
    val handler = new AdapterProxyChannelHandler({ pipeline =>
      pipeline.addLast(new Aggregator())
    })
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(handler)

    channel.writeOutbound(Message(request, 3))
    channel.writeOutbound(Message(request, 5))
    assert(channel.outboundMessages.isEmpty)

    channel.writeOutbound(Message(dataReq, 3))
    assert(validateObject(channel.readOutbound[Message](), 3, true, Some(moon)))
    assert(channel.outboundMessages.isEmpty)

    channel.writeOutbound(Message(starsReq, 5))
    assert(validateObject(channel.readOutbound[Message](), 5, true, Some(stars)))
    assert(channel.outboundMessages.isEmpty)
  }

  test("streams are torn down eventually") {
    val handler = new AdapterProxyChannelHandler(http.initClient(Params.empty))
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(handler)

    channel.writeOutbound(messageReq)
    assert(handler.numConnections == 1)
    channel.writeInbound(messageRep)
    assert(handler.numConnections == 0)
  }

  test("streams aren't torn down until we actually get a last item") {
    val stats = new InMemoryStatsReceiver
    val handler = new AdapterProxyChannelHandler(http.initClient(Params.empty), stats)
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(handler)

    val req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "twitter.com")
    val rep = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    val last = new DefaultLastHttpContent()
    val content = chunk()
    assert(content.refCnt() == 1)

    channel.writeOutbound(Message(req, 3))
    assert(handler.numConnections == 1)
    channel.writeInbound(Message(rep, 3))
    assert(handler.numConnections == 1)
    channel.writeInbound(Message(last, 3))
    channel.writeInbound(Message(new DefaultHttpContent(content), 3))
    assert(handler.numConnections == 1)
    channel.writeOutbound(Message(last, 3))
    assert(handler.numConnections == 0)
    assert(content.refCnt() == 0)
  }

  test("streams are torn down when we receive rst") {
    val handler = new AdapterProxyChannelHandler(http.initClient(Params.empty))
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(handler)

    val req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "twitter.com")
    val rep = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    val last = new DefaultLastHttpContent()
    channel.writeOutbound(Message(req, 3))
    assert(handler.numConnections == 1)
    channel.writeInbound(Message(rep, 3))
    val content = chunk()
    assert(content.refCnt() == 1)
    channel.writeInbound(Message(new DefaultHttpContent(content), 3))
    assert(handler.numConnections == 1)
    channel.writeInbound(Rst(3, 0x8))
    assert(content.refCnt() == 0)
    assert(handler.numConnections == 0)
    assert(last.refCnt() == 1)
    channel.writeInbound(Message(last, 3))
    assert(handler.numConnections == 0)
    assert(last.refCnt() == 0)
  }

  test("embedded handlers can cleanup on channel close") {
    // use actual client init fn
    val apch = new AdapterProxyChannelHandler(http.initClient(Params.empty))
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(apch)
    val content = chunk()
    assert(content.refCnt() == 1)
    // reading registers a new stream with a partially aggregated payload
    channel.pipeline().fireChannelRead(Message(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK), 3))
    channel.pipeline().fireChannelRead(Message(new DefaultHttpContent(content), 3))

    // session dies
    channel.close()

    assert(content.refCnt() == 0)
  }

  test("streams are torn down when we send rst") {
    val handler = new AdapterProxyChannelHandler(http.initClient(Params.empty))
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(handler)

    val req = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "twitter.com")
    val repChunk = Unpooled.wrappedBuffer("ref-counting on the jvm is a great idea".getBytes(StandardCharsets.UTF_8))
    assert(repChunk.refCnt() == 1)
    val rep = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
    val last = new DefaultLastHttpContent()

    channel.writeOutbound(Message(req, 3))
    assert(handler.numConnections == 1)
    channel.writeInbound(Message(rep, 3))
    channel.writeInbound(Message(new DefaultHttpContent(repChunk), 3))
    assert(handler.numConnections == 1)
    channel.writeInbound(Rst(3, 0x8))
    assert(handler.numConnections == 0)
    intercept[AdapterProxyChannelHandler.WriteToNackedStreamException] {
      channel.writeOutbound(Message(last, 3))
    }
    assert(repChunk.refCnt() == 0)
  }

  test("channel gauge is accurate") {
    val stats = new InMemoryStatsReceiver()
    val handler = new AdapterProxyChannelHandler(http.initClient(Params.empty), stats)
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(handler)

    val channelsGauge = stats.gauges(Seq("channels"))
    assert(channelsGauge() == 0.0)

    channel.writeOutbound(Message(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "twitter.com"), 3))
    channel.writeOutbound(Message(new DefaultLastHttpContent(), 3))
    assert(channelsGauge() == 1.0)

    channel.writeOutbound(Message(new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "twitter.com"), 5))
    assert(channelsGauge() == 2.0)

    channel.writeOutbound(Message(new DefaultLastHttpContent(), 5))
    channel.writeInbound(Message(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK), 3))
    channel.writeInbound(Message(new DefaultLastHttpContent(), 3))
    assert(channelsGauge() == 1.0)

    channel.writeInbound(Message(new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK), 5))
    channel.writeInbound(Message(new DefaultLastHttpContent(), 5))
    assert(channelsGauge() == 0.0)
  }

  test("close closes the underlying connection") {
    val handler = new AdapterProxyChannelHandler(http.initClient(Params.empty))
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(handler)

    channel.close()
    assert(!channel.isOpen)
  }

  test("fails hard when you try to write a StreamException") {
    val handler = new AdapterProxyChannelHandler({ pipeline =>
      pipeline.addLast(new Disaggregator())
    })
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(handler)
    val error = Http2Error.PROTOCOL_ERROR
    val hlsExn = headerListSizeError(3, error, true /* onDecode */, "too big") match {
      case e: HeaderListSizeException => e
      case _ => fail
    }

    val exn = StreamException(hlsExn, 3)
    val e = intercept[IllegalArgumentException] {
      channel.writeOutbound(exn)
    }
    intercept[HeaderListSizeException] {
      throw e.getCause()
    }
  }

  test("propagates StreamExceptions when reading") {
    val handler = new AdapterProxyChannelHandler({ pipeline =>
      pipeline.addLast(new Disaggregator())
    })
    val channel = new EmbeddedChannel()
    channel.pipeline.addLast(handler)
    val error = Http2Error.PROTOCOL_ERROR
    val hlsExn = headerListSizeError(3, error, true /* onDecode */, "too big") match {
      case e: HeaderListSizeException => e
      case _ => fail
    }

    val exn = StreamException(hlsExn, 3)
    channel.writeInbound(exn)
    val read = channel.readInbound[StreamException]()
    assert(read.equals(exn))
  }

}

class Aggregator extends ChannelDuplexHandler {
  var curWrite: Option[(HttpRequest, ChannelPromise)] = None
  var curRead: Option[HttpRequest] = None

  override def write(ctx: ChannelHandlerContext, msg: Object, promise: ChannelPromise): Unit = {
    (curWrite, msg) match {
      case (Some((x, oldP)), data: HttpContent) =>
        promise.addListener(new ChannelPromiseNotifier(oldP))
        ctx.write(
          new DefaultFullHttpRequest(
            x.getProtocolVersion,
            x.getMethod,
            x.getUri,
            data.content
          ),
          promise
        )
        curWrite = None
      case (None, req: HttpRequest) =>
        curWrite = Some((req, promise))
      case _ =>
        throw new Exception("boom! expected a different message.")
    }
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
    (curRead, msg) match {
      case (Some(x), data: HttpContent) =>
        ctx.fireChannelRead(
          new DefaultFullHttpRequest(x.getProtocolVersion, x.getMethod, x.getUri, data.content)
        )
        curRead = None
      case (None, req: HttpRequest) =>
        curRead = Some(req)
      case _ =>
        throw new Exception("boom! expected a different message.")
    }
  }
}

class Disaggregator extends ChannelDuplexHandler {
  override def write(ctx: ChannelHandlerContext, msg: Object, promise: ChannelPromise): Unit = {
    msg match {
      case fullReq: FullHttpRequest =>
        val req = new DefaultHttpRequest(
          fullReq.getProtocolVersion,
          fullReq.getMethod,
          fullReq.getUri
        )
        val data = new DefaultHttpContent(fullReq.content)
        val combiner = new PromiseCombiner()
        val reqP = ctx.newPromise()
        val dataP = ctx.newPromise()
        combiner.add(reqP)
        combiner.add(dataP)
        combiner.finish(promise)
        ctx.write(req, reqP)
        ctx.write(data, dataP)
    }
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: Object): Unit = {
    msg match {
      case fullReq: FullHttpRequest =>
        val req = new DefaultHttpRequest(
          fullReq.getProtocolVersion,
          fullReq.getMethod,
          fullReq.getUri
        )
        val data = new DefaultHttpContent(fullReq.content)
        ctx.fireChannelRead(req)
        ctx.fireChannelRead(data)
    }
  }
}
