package com.twitter.finagle.thrift

import org.specs.Specification
import org.specs.matcher.Matcher

import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

import org.apache.thrift.{TBase, TApplicationException}
import org.apache.thrift.protocol.{
  TProtocol, TBinaryProtocol, TMessage, TMessageType}
import org.apache.thrift.transport.TTransportException

import com.twitter.finagle.SunkChannel
import com.twitter.finagle.channel.TooManyDicksOnTheDanceFloorException
import com.twitter.silly.Silly

object ThriftCodecSpec extends Specification {
  def thriftToBuffer(method: String, `type`: Byte, seqid: Int,
      message: { def write(p: TProtocol) }): ChannelBuffer = {
    val buffer = ChannelBuffers.dynamicBuffer()
    val transport = new ChannelBufferTransport(buffer)
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

      val channel = makeChannel(new ThriftServerEncoder)
      Channels.write(channel, call.reply(reply))

      channel.upstreamEvents must haveSize(0)
      channel.downstreamEvents must haveSize(1)

      val message   = channel.downstreamEvents(0).asInstanceOf[MessageEvent].getMessage()
      val buffer    = message.asInstanceOf[ChannelBuffer]
      val transport = new ChannelBufferTransport(buffer)
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

  "thrift server framed decoder" should {
    "decode calls" in {
      // receive call and decode
      val buffer = thriftToBuffer("bleep", TMessageType.CALL, 23, new Silly.bleep_args("args"))
      val channel = makeChannel(new ThriftFramedServerDecoder)
      Channels.fireMessageReceived(channel, buffer)
      channel.upstreamEvents must haveSize(1)
      channel.downstreamEvents must haveSize(0)

      // verify decode
      val message = channel.upstreamEvents(0).asInstanceOf[MessageEvent].getMessage()
      val call = message.asInstanceOf[ThriftCall[Silly.bleep_args, Silly.bleep_result]]
      call mustNot beNull

      call.method            must be_==("bleep")
      call.seqid             must be_==(23)
      call.arguments.request must be_==("args")
    }

    "fail to decode non-call message types" in {
      // receive non-call and decode
      val buffer = thriftToBuffer("bleep", TMessageType.REPLY, 23, new Silly.bleep_args("args"))
      val channel = makeChannel(new ThriftFramedServerDecoder)
      Channels.fireMessageReceived(channel, buffer)

      // verify throws TApplicationException(INVALID_MESSAGE_TYPE)
      channel.upstreamEvents must haveSize(1)
      channel.downstreamEvents must haveSize(0)
      val event = channel.upstreamEvents(0).asInstanceOf[ExceptionEvent]
      event.getCause must haveClass[TApplicationException]
      val ex = event.getCause.asInstanceOf[TApplicationException]
      ex.getType mustEqual TApplicationException.INVALID_MESSAGE_TYPE
    }

    "fail to decode unknown method calls" in {
      // receive call for unknown method and decode
      val buffer = thriftToBuffer("unknown", TMessageType.CALL, 23, new Silly.bleep_args("args"))
      val channel = makeChannel(new ThriftFramedServerDecoder)
      Channels.fireMessageReceived(channel, buffer)

      // verify throws TApplicationException(UNKNOWN_METHOD)
      channel.upstreamEvents must haveSize(1)
      channel.downstreamEvents must haveSize(0)
      val event = channel.upstreamEvents(0).asInstanceOf[ExceptionEvent]
      event.getCause must haveClass[TApplicationException]
      val ex = event.getCause.asInstanceOf[TApplicationException]
      ex.getType mustEqual TApplicationException.UNKNOWN_METHOD
    }

    "fail to decode truncated calls" in {
      val buffer = thriftToBuffer("bleep", TMessageType.CALL, 23, new Silly.bleep_args("args"))

      Range(0, buffer.readableBytes - 1).foreach { numBytes =>
        // receive truncated call
        val channel = makeChannel(new ThriftFramedServerDecoder)
        val truncatedBuffer = buffer.copy(buffer.readerIndex, numBytes)
        Channels.fireMessageReceived(channel, truncatedBuffer)

        // TTransportException should be thrown
        channel.upstreamEvents must haveSize(1)
        channel.downstreamEvents must haveSize(0)
        val event = channel.upstreamEvents(0).asInstanceOf[ExceptionEvent]
        event.getCause must haveClass[TTransportException]
      }
    }
  }

  "thrift server unframed decoder" should {
    "decode calls" in {
      // receive call and decode
      val buffer = thriftToBuffer("bleep", TMessageType.CALL, 23, new Silly.bleep_args("args"))
      val channel = makeChannel(new ThriftUnframedServerDecoder)
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
        val channel = makeChannel(new ThriftUnframedServerDecoder)
        val truncatedBuffer = buffer.copy(buffer.readerIndex, numBytes)
        Channels.fireMessageReceived(channel, truncatedBuffer)

        // event should be ignored
        channel.upstreamEvents must haveSize(0)
        channel.downstreamEvents must haveSize(0)

        // receive remainder of call
        truncatedBuffer.writeBytes(buffer, buffer.readerIndex + numBytes,
                                   buffer.readableBytes - numBytes)
        Channels.fireMessageReceived(channel, truncatedBuffer)

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
      val channel = makeChannel(new ThriftClientEncoder)
      Channels.write(channel, call)

      channel.upstreamEvents must haveSize(0)
      channel.downstreamEvents must haveSize(1)

      val message   = channel.downstreamEvents(0).asInstanceOf[MessageEvent].getMessage()
      val buffer    = message.asInstanceOf[ChannelBuffer]
      val transport = new ChannelBufferTransport(buffer)
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

  "thrift client framed decoder" should {
    "decode replys" in {
      // receive reply and decode
      val buffer = thriftToBuffer("bleep", TMessageType.REPLY, 23, new Silly.bleep_result("result"))
      val channel = makeChannel(new ThriftFramedClientDecoder)
      Channels.fireMessageReceived(channel, buffer)
      channel.upstreamEvents must haveSize(1)
      channel.downstreamEvents must haveSize(0)

      // verify decode
      val message = channel.upstreamEvents(0).asInstanceOf[MessageEvent].getMessage()
      val result = message.asInstanceOf[Silly.bleep_result]
      result mustNot beNull
      result.isSetSuccess must beTrue
      result.success must be_==("result")
    }

    "decode exceptions" in {
      // receive exception and decode
      val buffer = thriftToBuffer("bleep", TMessageType.EXCEPTION, 23,
        new TApplicationException(TApplicationException.UNKNOWN_METHOD, "message"))
      val channel = makeChannel(new ThriftFramedClientDecoder)
      Channels.fireMessageReceived(channel, buffer)
      channel.upstreamEvents must haveSize(1)
      channel.downstreamEvents must haveSize(0)

      // TApplicationException should be thrown
      val event = channel.upstreamEvents(0).asInstanceOf[DefaultExceptionEvent]
      val cause = event.getCause.asInstanceOf[TApplicationException]
      cause.getType    mustEqual TApplicationException.UNKNOWN_METHOD
      cause.getMessage mustEqual "message"
    }

    "send invalid message type exceptions for unsupported message types" in {
      val invalid_message_type = 99.toByte
      val buffer = thriftToBuffer("bleep", invalid_message_type, 23,
        new TApplicationException(TApplicationException.UNKNOWN_METHOD, "message"))
      val channel = makeChannel(new ThriftFramedClientDecoder)
      Channels.fireMessageReceived(channel, buffer)
      channel.upstreamEvents must haveSize(1)
      channel.downstreamEvents must haveSize(0)

      // TApplicationException should be thrown
      val event = channel.upstreamEvents(0).asInstanceOf[DefaultExceptionEvent]
      val cause = event.getCause.asInstanceOf[TApplicationException]
      cause.getType    mustEqual TApplicationException.INVALID_MESSAGE_TYPE
    }

    "fail on truncated replys" in {
      val buffer = thriftToBuffer("bleep", TMessageType.REPLY, 23, new Silly.bleep_result("result"))

      Range(0, buffer.readableBytes - 1).foreach { numBytes =>
        // receive truncated call
        val channel = makeChannel(new ThriftFramedClientDecoder)
        val truncatedBuffer = buffer.copy(buffer.readerIndex, numBytes)
        Channels.fireMessageReceived(channel, truncatedBuffer)

        // TTransportException should be thrown
        channel.upstreamEvents must haveSize(1)
        channel.downstreamEvents must haveSize(0)
        val event = channel.upstreamEvents(0).asInstanceOf[ExceptionEvent]
        event.getCause must haveClass[TTransportException]
      }
    }
  }

  "thrift client unframed decoder" should {
    "decode replys" in {
      // receive reply and decode
      val buffer = thriftToBuffer("bleep", TMessageType.REPLY, 23, new Silly.bleep_result("result"))
      val channel = makeChannel(new ThriftUnframedClientDecoder)
      Channels.fireMessageReceived(channel, buffer)
      channel.upstreamEvents must haveSize(1)
      channel.downstreamEvents must haveSize(0)

      // verify decode
      val message = channel.upstreamEvents(0).asInstanceOf[MessageEvent].getMessage()
      val result = message.asInstanceOf[Silly.bleep_result]
      result mustNot beNull
      result.isSetSuccess must beTrue
      result.success must be_==("result")
    }

    "decode replys broken in two" in {
      val buffer = thriftToBuffer("bleep", TMessageType.REPLY, 23, new Silly.bleep_result("result"))

      Range(0, buffer.readableBytes - 1).foreach { numBytes =>
        // receive partial call
        val channel = makeChannel(new ThriftUnframedClientDecoder)
        val truncatedBuffer = buffer.copy(buffer.readerIndex, numBytes)
        Channels.fireMessageReceived(channel, truncatedBuffer)

        // event should be ignored
        channel.upstreamEvents must haveSize(0)
        channel.downstreamEvents must haveSize(0)

        // receive remainder of call
        truncatedBuffer.writeBytes(buffer, buffer.readerIndex + numBytes,
                                   buffer.readableBytes - numBytes)
        Channels.fireMessageReceived(channel, truncatedBuffer)

        // call should be received
        channel.upstreamEvents must haveSize(1)
        channel.downstreamEvents must haveSize(0)
        val message = channel.upstreamEvents(0).asInstanceOf[MessageEvent].getMessage()
        val result = message.asInstanceOf[Silly.bleep_result]
        result mustNot beNull
        result.isSetSuccess must beTrue
        result.success must be_==("result")
      }
    }
  }
}
