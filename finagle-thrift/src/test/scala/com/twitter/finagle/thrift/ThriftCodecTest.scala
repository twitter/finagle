package com.twitter.finagle.thrift

import com.twitter.finagle.SunkChannel
import com.twitter.silly.Silly
import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ThriftCodecTest extends FunSuite {

  def thriftToBuffer(
    method: String,
    `type`: Byte,
    seqId: Int,
    message: { def write(p: TProtocol) }
  ): ChannelBuffer = {
    val buffer = ChannelBuffers.dynamicBuffer()
    val transport = new ChannelBufferToTransport(buffer)
    val protocol = new TBinaryProtocol(transport, true, true)
    protocol.writeMessageBegin(new TMessage(method, `type`, seqId))
    message.write(protocol)
    protocol.writeMessageEnd()
    buffer
  }

  def makeChannel(codec: ChannelHandler) = SunkChannel {
    val pipeline = Channels.pipeline()
    pipeline.addLast("codec", codec)
    pipeline
  }

  ThriftTypes.add(new ThriftCallFactory[Silly.bleep_args, Silly.bleep_result](
    "bleep", classOf[Silly.bleep_args], classOf[Silly.bleep_result]))

  test("thrift server encoder should encode replys") {
    val protocolFactory = new TBinaryProtocol.Factory()
    val call = new ThriftCall("testMethod", new Silly.bleep_args("arg"), classOf[Silly.bleep_result], 23)
    val reply = call.newReply
    reply.setSuccess("result")

    val channel = makeChannel(new ThriftServerEncoder(protocolFactory))
    Channels.write(channel, call.reply(reply))

    assert(channel.upstreamEvents.size == 0)
    assert(channel.downstreamEvents.size == 1)

    val message   = channel.downstreamEvents(0).asInstanceOf[MessageEvent].getMessage()
    val buffer    = message.asInstanceOf[ChannelBuffer]
    val transport = new ChannelBufferToTransport(buffer)
    val protocol  = new TBinaryProtocol(transport, true, true)

    val tmessage = protocol.readMessageBegin()
    assert(tmessage.`type` == TMessageType.REPLY)
    assert(tmessage.name   == "testMethod")
    assert(tmessage.seqid  == 23)

    val result = new Silly.bleep_result()
    result.read(protocol)
    assert(result.isSetSuccess)
    assert(result.success == "result")
  }

  test("thrift server decoder should decode calls") {
    val protocolFactory = new TBinaryProtocol.Factory()
    // receive call and decode
    val buffer = thriftToBuffer("bleep", TMessageType.CALL, 23, new Silly.bleep_args("args"))
    val channel = makeChannel(new ThriftServerDecoder(protocolFactory))
    Channels.fireMessageReceived(channel, buffer)
    assert(channel.upstreamEvents.size == 1)
    assert(channel.downstreamEvents.size == 0)

    // verify decode
    val message = channel.upstreamEvents(0).asInstanceOf[MessageEvent].getMessage()
    val thriftCall = message.asInstanceOf[ThriftCall[Silly.bleep_args, Silly.bleep_result]]
    assert(thriftCall != null)

    assert(thriftCall.method == "bleep")
    assert(thriftCall.seqid == 23)
    assert(thriftCall.arguments.request == "args")
  }

  test("thrift server decoder should decode calls broken in two") {
    val protocolFactory = new TBinaryProtocol.Factory()
    val buffer = thriftToBuffer("bleep", TMessageType.CALL, 23, new Silly.bleep_args("args"))

    Range(0, buffer.readableBytes - 1).foreach { numBytes =>
      // receive partial call
      val channel = makeChannel(new ThriftServerDecoder(protocolFactory))
      val truncatedBuffer = buffer.copy(buffer.readerIndex, numBytes)
      Channels.fireMessageReceived(channel, truncatedBuffer)

      // event should be ignored
      assert(channel.upstreamEvents.size == 0)
      assert(channel.downstreamEvents.size == 0)

      val remainder = buffer.copy(buffer.readerIndex+numBytes, buffer.readableBytes-numBytes)
      // receive remainder of call
      Channels.fireMessageReceived(channel, remainder)

      // call should be received
      assert(channel.upstreamEvents.size == 1)
      assert(channel.downstreamEvents.size == 0)

      val message = channel.upstreamEvents(0).asInstanceOf[MessageEvent].getMessage()
      val thriftCall = message.asInstanceOf[ThriftCall[Silly.bleep_args, Silly.bleep_result]]
      assert(thriftCall != null)
      assert(thriftCall.method == "bleep")
    }
  }

  test("thrift client encoder should encode calls") {
    val protocolFactory = new TBinaryProtocol.Factory()
    val call = new ThriftCall("testMethod", new Silly.bleep_args("arg"), classOf[Silly.bleep_result])
    val channel = makeChannel(new ThriftClientEncoder(protocolFactory))
    Channels.write(channel, call)

    assert(channel.upstreamEvents.size == 0)
    assert(channel.downstreamEvents.size == 1)

    val message   = channel.downstreamEvents(0).asInstanceOf[MessageEvent].getMessage()
    val buffer    = message.asInstanceOf[ChannelBuffer]
    val transport = new ChannelBufferToTransport(buffer)
    val protocol  = new TBinaryProtocol(transport, true, true)

    val tmessage = protocol.readMessageBegin()
    assert(tmessage.`type` == TMessageType.CALL)
    assert(tmessage.name   == "testMethod")
    assert(tmessage.seqid  == 1)

    val args = new Silly.bleep_args()
    args.read(protocol)
    assert(args.request == "arg")
  }

  test("thrift client decoder should decode replys") {
    val protocolFactory = new TBinaryProtocol.Factory()
    // receive reply and decode
    val buffer = thriftToBuffer("bleep", TMessageType.REPLY, 23, new Silly.bleep_result("result"))
    val channel = makeChannel(new ThriftClientDecoder(protocolFactory))
    Channels.fireMessageReceived(channel, buffer)
    assert(channel.upstreamEvents.size == 1)
    assert(channel.downstreamEvents.size == 0)

    // verify decode
    val message = channel.upstreamEvents(0).asInstanceOf[MessageEvent].getMessage()
    val result = message.asInstanceOf[ThriftReply[Silly.bleep_result]]
    assert(result != null)
    assert(result.response.success == "result")
  }

  test("decode replies broken in two") {
    val protocolFactory = new TBinaryProtocol.Factory()
    val buffer = thriftToBuffer("bleep", TMessageType.REPLY, 23, new Silly.bleep_result("result"))

    Range(0, buffer.readableBytes - 1).foreach { numBytes =>
      // receive partial call
      val channel = makeChannel(new ThriftClientDecoder(protocolFactory))
      val truncatedBuffer = buffer.copy(buffer.readerIndex, numBytes)
      Channels.fireMessageReceived(channel, truncatedBuffer)

      // event should be ignored
      assert(channel.upstreamEvents.size == 0)
      assert(channel.downstreamEvents.size == 0)

      val remainder = buffer.copy(buffer.readerIndex+numBytes, buffer.readableBytes-numBytes)
      // receive remainder of call
      Channels.fireMessageReceived(channel, remainder)

      // call should be received
      assert(channel.upstreamEvents.size == 1)
      assert(channel.downstreamEvents.size == 0)
      val message = channel.upstreamEvents(0).asInstanceOf[MessageEvent].getMessage()
      val result = message.asInstanceOf[ThriftReply[Silly.bleep_result]]
      assert(result != null)
      assert(result.response.success == "result")
    }
  }

  test("decode exceptions") {
    val protocolFactory = new TBinaryProtocol.Factory()
    // receive exception and decode
    val buffer = thriftToBuffer("bleep", TMessageType.EXCEPTION, 23,
      new TApplicationException(TApplicationException.UNKNOWN_METHOD, "message"))
    val channel = makeChannel(new ThriftClientDecoder(protocolFactory))
    Channels.fireMessageReceived(channel, buffer)
    assert(channel.upstreamEvents.size == 1)
    assert(channel.downstreamEvents.size == 0)

    // TApplicationException should be thrown
    val event = channel.upstreamEvents(0).asInstanceOf[DefaultExceptionEvent]
    val cause = event.getCause.asInstanceOf[TApplicationException]
    assert(cause.getType == TApplicationException.UNKNOWN_METHOD)
    assert(cause.getMessage == "message")
  }
}
