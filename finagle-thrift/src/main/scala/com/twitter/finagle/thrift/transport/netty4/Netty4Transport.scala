package com.twitter.finagle.thrift.transport.netty4

import com.twitter.finagle.{Stack, Thrift}
import com.twitter.finagle.client.Transporter
import com.twitter.finagle.netty4.{DirectToHeapInboundHandlerName, Netty4Listener, Netty4Transporter}
import com.twitter.finagle.param.Label
import com.twitter.finagle.server.Listener
import com.twitter.finagle.thrift.ThriftClientRequest
import com.twitter.logging.Logger
import io.netty.channel.ChannelPipeline


/**
 * Netty4 [[Transporter]] and [[Listener]] builder implementations
 *
 * The method of message segmentation and the thrift protocol are obtained from the provided
 * [[Stack.Params]]. See the [[Thrift]] object for more details.
 *
 */

private[finagle] object Netty4Transport {

  private[this] val logger = Logger.get(this.getClass)

  val Client: Stack.Params => Transporter[ThriftClientRequest, Array[Byte]] = { params =>
    val pipeline = { pipeline: ChannelPipeline =>
      addFramerAtLast(pipeline, params)
      pipeline.addLast("clientByteCodec", ClientByteBufCodec())
      ()
    }

    Netty4Transporter(pipeline, params)
  }

  val Server: Stack.Params => Listener[Array[Byte], Array[Byte]] = { params =>
    val Thrift.param.Framed(framed) = params[Thrift.param.Framed]

    val pipeline ={ pipeline: ChannelPipeline =>
      addFramerAtLast(pipeline, params)
      pipeline.addLast("serverByteCodec", ServerByteBufCodec())
      ()
    }

    Netty4Listener[Array[Byte], Array[Byte]](pipeline,
      if (params.contains[Label]) params else params + Label("thrift"))
  }

  // Add a framed codec or buffered decoded based on the provided stack params
  private def addFramerAtLast(pipeline: ChannelPipeline, params: Stack.Params): Unit = {
    val Thrift.param.Framed(framed) = params[Thrift.param.Framed]
    if (framed) {
      pipeline.addLast("thriftFrameCodec", ThriftFrameCodec())
    } else {
      // use the buffered transport framer
      val Thrift.param.ProtocolFactory(protocolFactory) = params[Thrift.param.ProtocolFactory]
      pipeline.addLast("thriftBufferDecoder", new ThriftBufferedTransportDecoder(protocolFactory))
    }

    // We don't need it because all the netty `ByteBuf` types will be released by
    // the `Array[Byte]` codecs
    try pipeline.remove(DirectToHeapInboundHandlerName)
    catch { case _: NoSuchElementException =>
      logger.info("Expected DirectToHeapInboundHandler in the netty4 channel pipeline, " +
        "but it didn't exist.")
    }
  }
}