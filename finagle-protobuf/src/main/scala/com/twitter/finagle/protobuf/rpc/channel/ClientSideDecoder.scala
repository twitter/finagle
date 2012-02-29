package com.twitter.finagle.protobuf.rpc.channel

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBufferInputStream
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.handler.codec.frame.FrameDecoder

import com.google.protobuf.Message
import com.google.protobuf.Service

class ClientSideDecoder(val repo: MethodLookup, val service: Service) extends FrameDecoder with ProtobufDecoder {

  @throws(classOf[Exception])
  def decode(ctx: ChannelHandlerContext, channel: Channel, buf: ChannelBuffer): Object = {
    decode(ctx, channel, buf, repo)
  }

  def getPrototype(methodName: String): Message = {
    val m = service.getDescriptorForType().findMethodByName(methodName)
    service.getResponsePrototype(m)
  }
}
