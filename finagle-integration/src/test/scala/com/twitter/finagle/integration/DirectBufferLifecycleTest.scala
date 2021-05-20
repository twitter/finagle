package com.twitter.finagle.integration

import com.twitter.finagle.Stack
import com.twitter.finagle.integration.thriftscala.Echo
import com.twitter.finagle.memcached.protocol.Value
import com.twitter.finagle.memcached.protocol.text.server.ResponseToBuf
import com.twitter.finagle.memcached.protocol.text.transport.MemcachedNetty4ClientPipelineInit
import com.twitter.finagle.memcached.{protocol => memcached}
import com.twitter.finagle.thrift.transport.{netty4 => thrift}
import com.twitter.io.Buf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelPipeline
import io.netty.channel.embedded.EmbeddedChannel
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.{TFramedTransport, TMemoryBuffer}
import org.scalatest.funsuite.AnyFunSuite

/**
 * The majority of finagle protocols manage inbound direct buffers in their netty pipeline.
 *
 * Covered protocols:
 *
 *  kestrel (implicitly via memcached)
 *  memcached
 *  thrift
 *  mysql (coverage from c.t.f.netty4.channel.Netty4ClientChannelInitializerTest)
 *  redis (uses framed n4 channel init so coverage comes from c.t.f.n4.channel.Netty4ClientChannelInitializerTest)
 *  http/1.1, http/2 (coverage from c.t.f.http.DirectPayloadsLifecycleTest)
 */
class DirectBufferLifecycleTest extends AnyFunSuite {

  /**
   * @tparam T the framed protocol type
   */
  def testDirect[T](protocol: String, msg: Buf, pipelineInit: (ChannelPipeline => Unit)) =
    test(s"$protocol framer releases inbound direct byte bufs") {
      val e = new EmbeddedChannel()
      pipelineInit(e.pipeline)
      val direct = Unpooled.directBuffer()
      direct.writeBytes(Buf.ByteArray.Owned.extract(msg))
      assert(direct.refCnt() == 1)
      e.writeInbound(direct)
      assert(direct.refCnt() == 0)
      val framed: T = e.readInbound[T]()
    }

  testDirect[Buf](
    protocol = "memcached server",
    msg = {
      val cte = new memcached.text.client.CommandToBuf
      val command = memcached.Get(Seq(Buf.Utf8("1")))
      cte.encode(command)
    },
    pipelineInit = memcached.text.transport.Netty4ServerFramer
  )

  testDirect[Buf](
    protocol = "memcached client",
    msg = ResponseToBuf.encode(
      memcached.Values(Seq(Value(Buf.Utf8("key"), Buf.Utf8("1"), None, None)))
    ),
    pipelineInit = MemcachedNetty4ClientPipelineInit
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
