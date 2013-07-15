package com.twitter.finagle.thrift

import org.specs.SpecificationWithJUnit

import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol.{
  TProtocol, TBinaryProtocol, TMessage, TMessageType}

import com.twitter.finagle.SunkChannel
import com.twitter.silly.Silly

class ThriftCodecSpec extends SpecificationWithJUnit {
  val protocolFactory = new TBinaryProtocol.Factory()

  def thriftToBuffer(method: String, `type`: Byte, seqid: Int,
      message: { def write(p: TProtocol) }): ChannelBuffer = {
    val buffer = ChannelBuffers.dynamicBuffer()
    val transport = new ChannelBufferToTransport(buffer)
    val protocol = new TBinaryProtocol(transport, true, true)
    protocol.writeMessageBegin(new TMessage(method, `type`, seqid))
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

  "thrift server encoder" should {
    "encode replys" in {
      val call = new ThriftCall("testMethod", new Silly.bleep_args("arg"), classOf[Silly.bleep_result], 23)
      val reply = call.newReply
      reply.setSuccess("result")

      val channel = makeChannel(new ThriftServerEncoder(protocolFactory))
      Channels.write(channel, call.reply(reply))

      channel.upstreamEvents must haveSize(0)
      channel.downstreamEvents must haveSize(1)

      val message   = channel.downstreamEvents(0).asInstanceOf[MessageEvent].getMessage()
      val buffer    = message.asInstanceOf[ChannelBuffer]
      val transport = new ChannelBufferToTransport(buffer)
      val protocol  = new TBinaryProtocol(transport, true, true)

      val tmessage = protocol.readMessageBegin()
      tmessage.`type` must be_==(TMessageType.REPLY)
      tmessage.name   must be_==("testMethod")
      tmessage.seqid  must be_==(23)

      val result = new Silly.bleep_result()
      result.read(protocol)
      result.isSetSuccess must beTrue
      result.success must be_==("result")
    }
  }

  "thrift server decoder" should {
    "decode calls" in {
      // receive call and decode
      val buffer = thriftToBuffer("bleep", TMessageType.CALL, 23, new Silly.bleep_args("args"))
      val channel = makeChannel(new ThriftServerDecoder(protocolFactory))
      Channels.fireMessageReceived(channel, buffer)
      channel.upstreamEvents must haveSize(1)
      channel.downstreamEvents must haveSize(0)

      // verify decode
      val message = channel.upstreamEvents(0).asInstanceOf[MessageEvent].getMessage()
      val thriftCall = message.asInstanceOf[ThriftCall[Silly.bleep_args, Silly.bleep_result]]
      thriftCall mustNot beNull

      thriftCall.method            must be_==("bleep")
      thriftCall.seqid             must be_==(23)
      thriftCall.arguments.request must be_==("args")
    }

    "decode calls broken in two" in {
      val buffer = thriftToBuffer("bleep", TMessageType.CALL, 23, new Silly.bleep_args("args"))

      Range(0, buffer.readableBytes - 1).foreach { numBytes =>
        // receive partial call
        val channel = makeChannel(new ThriftServerDecoder(protocolFactory))
        val truncatedBuffer = buffer.copy(buffer.readerIndex, numBytes)
        Channels.fireMessageReceived(channel, truncatedBuffer)

        // event should be ignored
        channel.upstreamEvents must haveSize(0)
        channel.downstreamEvents must haveSize(0)

        val remainder = buffer.copy(buffer.readerIndex+numBytes, buffer.readableBytes-numBytes)
        // receive remainder of call
        Channels.fireMessageReceived(channel, remainder)

        // call should be received
        channel.upstreamEvents must haveSize(1)
        channel.downstreamEvents must haveSize(0)

        val message = channel.upstreamEvents(0).asInstanceOf[MessageEvent].getMessage()
        val thriftCall = message.asInstanceOf[ThriftCall[Silly.bleep_args, Silly.bleep_result]]
        thriftCall mustNot beNull
        thriftCall.method must be_==("bleep")
      }
    }
  }

  "thrift client encoder" should {
    "encode calls" in {
      val call = new ThriftCall("testMethod", new Silly.bleep_args("arg"), classOf[Silly.bleep_result])
      val channel = makeChannel(new ThriftClientEncoder(protocolFactory))
      Channels.write(channel, call)

      channel.upstreamEvents must haveSize(0)
      channel.downstreamEvents must haveSize(1)

      val message   = channel.downstreamEvents(0).asInstanceOf[MessageEvent].getMessage()
      val buffer    = message.asInstanceOf[ChannelBuffer]
      val transport = new ChannelBufferToTransport(buffer)
      val protocol  = new TBinaryProtocol(transport, true, true)

      val tmessage = protocol.readMessageBegin()
      tmessage.`type` must be_==(TMessageType.CALL)
      tmessage.name   must be_==("testMethod")
      tmessage.seqid  must be_==(1)

      val args = new Silly.bleep_args()
      args.read(protocol)
      args.request must be_==("arg")
    }
  }

  "thrift client decoder" should {
    "decode replys" in {
      // receive reply and decode
      val buffer = thriftToBuffer("bleep", TMessageType.REPLY, 23, new Silly.bleep_result("result"))
      val channel = makeChannel(new ThriftClientDecoder(protocolFactory))
      Channels.fireMessageReceived(channel, buffer)
      channel.upstreamEvents must haveSize(1)
      channel.downstreamEvents must haveSize(0)

      // verify decode
      val message = channel.upstreamEvents(0).asInstanceOf[MessageEvent].getMessage()
      val result = message.asInstanceOf[ThriftReply[Silly.bleep_result]]
      result mustNot beNull
      result.response.success must be_==("result")
    }

    "decode replies broken in two" in {
      val buffer = thriftToBuffer("bleep", TMessageType.REPLY, 23, new Silly.bleep_result("result"))

      Range(0, buffer.readableBytes - 1).foreach { numBytes =>
        // receive partial call
        val channel = makeChannel(new ThriftClientDecoder(protocolFactory))
        val truncatedBuffer = buffer.copy(buffer.readerIndex, numBytes)
        Channels.fireMessageReceived(channel, truncatedBuffer)

        // event should be ignored
        channel.upstreamEvents must haveSize(0)
        channel.downstreamEvents must haveSize(0)

        val remainder = buffer.copy(buffer.readerIndex+numBytes, buffer.readableBytes-numBytes)
        // receive remainder of call
        Channels.fireMessageReceived(channel, remainder)

        // call should be received
        channel.upstreamEvents must haveSize(1)
        channel.downstreamEvents must haveSize(0)
        val message = channel.upstreamEvents(0).asInstanceOf[MessageEvent].getMessage()
        val result = message.asInstanceOf[ThriftReply[Silly.bleep_result]]
        result mustNot beNull
        result.response.success must be_==("result")
      }
    }

    "decode exceptions" in {
      // receive exception and decode
      val buffer = thriftToBuffer("bleep", TMessageType.EXCEPTION, 23,
        new TApplicationException(TApplicationException.UNKNOWN_METHOD, "message"))
      val channel = makeChannel(new ThriftClientDecoder(protocolFactory))
      Channels.fireMessageReceived(channel, buffer)
      channel.upstreamEvents must haveSize(1)
      channel.downstreamEvents must haveSize(0)

      // TApplicationException should be thrown
      val event = channel.upstreamEvents(0).asInstanceOf[DefaultExceptionEvent]
      val cause = event.getCause.asInstanceOf[TApplicationException]
      cause.getType    mustEqual TApplicationException.UNKNOWN_METHOD
      cause.getMessage mustEqual "message"
    }
  }
}
