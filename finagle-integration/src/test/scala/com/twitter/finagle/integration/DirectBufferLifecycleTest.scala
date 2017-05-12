package com.twitter.finagle.integration

import com.twitter.finagle.Stack
import com.twitter.finagle.integration.thriftscala.Echo
import com.twitter.finagle.memcached.{protocol => memcached}
import com.twitter.finagle.mux.{transport => mux}
import com.twitter.finagle.thrift.transport.{netty4 => thrift}
import com.twitter.io.Buf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelPipeline
import io.netty.channel.embedded.EmbeddedChannel
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
 *  kestrel (implicitly via memcached)
 *  memcached
 *  mux (copying framer only)
 *  thrift
 *
 *  mysql (coverage from c.t.f.netty4.channel.Netty4ClientChannelInitializerTest)
 *  http/1.1, http/2 (coverage from c.t.f.http.DirectPayloadsLifecycleTest)
 */
@RunWith(classOf[JUnitRunner])
class DirectBufferLifecycleTest extends FunSuite {

  /**
   * @tparam T the framed protocol type
   */
  def testDirect[T](
    protocol: String,
    msg: Buf,
    pipelineInit: (ChannelPipeline => Unit)
  ) = test(s"$protocol framer releases inbound direct byte bufs") {
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
    protocol = "mux client/server",
    msg = Buf.U32BE(4).concat(mux.Message.encode(mux.Message.Tping(123))),
    pipelineInit = mux.CopyingFramer
  )

  testDirect[Buf](
    protocol = "memcached server",
    msg = {
      val cte: memcached.text.AbstractCommandToEncoding[AnyRef] = new memcached.text.CommandToEncoding
      val enc = new memcached.text.Encoder
      val command = memcached.Get(Seq(Buf.Utf8("1")))
      enc.encode(cte.encode(command))
    },
    pipelineInit = memcached.text.transport.Netty4ServerFramer
  )

  testDirect[Buf](
    protocol = "memcached client",
    msg = {
      val cte = new memcached.text.CommandToEncoding[AnyRef]
      val enc = new memcached.text.Encoder
      val command = memcached.Get(Seq(Buf.Utf8("1")))
      enc.encode(cte.encode(command))
    },
    pipelineInit = memcached.text.transport.Netty4ClientFramer
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
