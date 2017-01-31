package com.twitter.finagle.integration

import com.twitter.finagle.Stack
import com.twitter.finagle.integration.thriftscala.Echo
import com.twitter.finagle.memcached.{protocol => memcached}
import com.twitter.finagle.mux.{transport => mux}
import com.twitter.finagle.netty4.{ByteBufAsBuf, http}
import com.twitter.finagle.thrift.transport.{netty4 => thrift}
import com.twitter.io.Buf
import io.netty.buffer.{ByteBufHolder, ByteBuf, Unpooled}
import io.netty.channel.ChannelPipeline
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.handler.codec.{http => nhttp}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TFramedTransport, TMemoryBuffer}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
 * The majority of finagle protocols manage inbound direct buffers in their netty pipeline.
 *
 * Covered protocols:
 *
 *  http 1.1
 *  todo: http2
 *  kestrel (implicitly via memcached)
 *  memcached
 *  mux (copying framer only)
 *  mysql (coverage from c.t.f.netty4.channel.Netty4ClientChannelInitializerTest)
 *  thrift
 *
 */
@RunWith(classOf[JUnitRunner])
class DirectBufferLifecycleTest extends FunSuite {

  /**
   * @tparam T the framed protocol type
   */
  def testDirect[T](
    protocol: String,
    msg: Buf,
    pipelineInit: (ChannelPipeline => Unit),
    framedCB: T => Unit = { _: Any => () }
  ) =
    test(s"$protocol framer releases inbound direct byte bufs") {
      val e = new EmbeddedChannel()
      pipelineInit(e.pipeline)
      val direct = Unpooled.directBuffer()
      direct.writeBytes(Buf.ByteArray.Owned.extract(msg))
      assert(direct.refCnt() == 1)
      e.writeInbound(direct)
      assert(direct.refCnt() == 0)
      val framed: T = e.readInbound[T]()
      framedCB(framed)
    }

  testDirect[ByteBufAsBuf](
    protocol = "mux client/server",
    msg = Buf.U32BE(4).concat(mux.Message.encode(mux.Message.Tping(123))),
    pipelineInit = mux.CopyingFramer,
    framedCB = { x => assert(!x.underlying.isDirect) }
  )

  testDirect[ByteBufHolder](
    protocol = "http 1.1 server",
    msg = {
      val content = "some content".getBytes("UTF-8")
      val req = new nhttp.DefaultFullHttpRequest(
        nhttp.HttpVersion.HTTP_1_1,
        nhttp.HttpMethod.GET,
        "/path",
        Unpooled.wrappedBuffer(content)
      )
      nhttp.HttpUtil.setContentLength(req, content.length)

      val e = new EmbeddedChannel()
      val enc = new nhttp.HttpRequestEncoder()
      e.pipeline.addLast(enc)
      assert(e.writeOutbound(req))
      ByteBufAsBuf.Owned(e.readOutbound[ByteBuf]())
    },
    pipelineInit = http.exp.ServerPipelineInit(Stack.Params.empty),
    framedCB = { x => assert(!x.content().isDirect) }
  )

  def http1Resp(): nhttp.HttpResponse =  {
    val resp =
      new nhttp.DefaultFullHttpResponse(
        nhttp.HttpVersion.HTTP_1_1,
        nhttp.HttpResponseStatus.OK,
        Unpooled.wrappedBuffer("pretty cool response".getBytes("UTF-8"))
      )
    nhttp.HttpUtil.setContentLength(resp, resp.content.readableBytes)
    resp
  }

  val http1RespBytes: ByteBuf = {
    val e = new EmbeddedChannel()
    val enc = new nhttp.HttpResponseEncoder()
    e.pipeline.addLast(enc)
    assert(e.writeOutbound(http1Resp()))
    e.readOutbound[ByteBuf]()
  }

  def http1Req(): nhttp.HttpRequest = {
    val req = new nhttp.DefaultFullHttpRequest(
      nhttp.HttpVersion.HTTP_1_1,
      nhttp.HttpMethod.GET,
      "/path",
      Unpooled.wrappedBuffer("rad request".getBytes("UTF-8"))
    )
    nhttp.HttpUtil.setContentLength(req, req.content.readableBytes)
    req
  }

  testDirect[ByteBufHolder](
    protocol = "http 1.1 client",
    msg = ByteBufAsBuf.Owned(http1RespBytes),
    pipelineInit = { (pipe: ChannelPipeline) =>
      http.exp.ClientPipelineInit(Stack.Params.empty)(pipe)
      pipe.writeAndFlush(http1Req())
    },
    framedCB = { x => assert(!x.content().isDirect) }
  )

  testDirect[ByteBufAsBuf](
    protocol = "memcached server",
    msg = {
      val cte: memcached.text.AbstractCommandToEncoding[AnyRef] = new memcached.text.CommandToEncoding
      val enc = new memcached.text.Encoder
      val command = memcached.Get(Seq(Buf.Utf8("1")))
      enc.encode(null, null, cte.encode(command))
    },
    pipelineInit = memcached.text.transport.Netty4ServerFramer,
    framedCB = { x => assert(!x.underlying.isDirect) }
  )

  testDirect[ByteBufAsBuf](
    protocol = "memcached client",
    msg = {
      val cte = new memcached.text.CommandToEncoding[AnyRef]
      val enc = new memcached.text.Encoder
      val command = memcached.Get(Seq(Buf.Utf8("1")))
      enc.encode(null, null, cte.encode(command))
    },
    pipelineInit = memcached.text.transport.Netty4ClientFramer,
    framedCB = { x => assert(!x.underlying.isDirect) }
  )

  testDirect[Array[Byte]](
    protocol = "thrift client",
    msg = {
      val buffer = new TMemoryBuffer(1)
      val framed = new TFramedTransport(buffer)
      val proto = new TBinaryProtocol(framed)
      val arg = Echo.Echo.Args("hi")
      Echo.Echo.Args.encode(arg, proto)
      framed.flush()
      val bytes = buffer.getArray
      Buf.ByteArray.Owned(bytes)
    },
    pipelineInit = thrift.Netty4Transport.ClientPipelineInit(Stack.Params.empty)
  )

  testDirect[Array[Byte]](
    protocol = "thrift server",
    msg = {
      val buffer = new TMemoryBuffer(1)
      val framed = new TFramedTransport(buffer)
      val proto = new TBinaryProtocol(framed)
      val resp = Echo.Echo.Result(Some("hello back"))
      Echo.Echo.Result.encode(resp, proto)
      framed.flush()
      val bytes = buffer.getArray
      Buf.ByteArray.Owned(bytes)
    },
    pipelineInit = thrift.Netty4Transport.ServerPipelineInit(Stack.Params.empty)
  )
}
