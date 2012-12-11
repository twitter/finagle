package com.twitter.finagle.thrift

import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.apache.thrift.TProcessorFactory
import org.apache.thrift.protocol.TProtocolFactory

private[thrift] class ThriftProcessorHandler(processorFactory: TProcessorFactory, protocolFactory: TProtocolFactory)
  extends SimpleChannelUpstreamHandler
{
  private def process(input: ChannelBuffer, output: ChannelBuffer) {
    val transport = new DuplexChannelBufferTransport(input, output)
    val protocol = protocolFactory.getProtocol(transport)
    val processor = processorFactory.getProcessor(transport)
    processor.process(protocol, protocol)
  }

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    e.getMessage match {
      case input: ChannelBuffer =>
        val output = ChannelBuffers.dynamicBuffer()
        process(input, output)
        Channels.write(ctx.getChannel, output)

      case x =>
        super.messageReceived(ctx, e)
    }
  }
}
