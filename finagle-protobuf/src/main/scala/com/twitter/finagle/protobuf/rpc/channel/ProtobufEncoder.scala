package com.twitter.finagle.protobuf.rpc.channel

import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.Channel
import org.jboss.netty.buffer.ChannelBuffers

import com.google.protobuf.Message

/**
 * Knows how to encode a ("doSomething()", ProtobufMessage) tuple.
 *
 * Message Format
 * ==============
 * 
 * Offset: 0             4                8
 *         +-------------+----------------+------------------+
 *         | method code | message length | protobuf message |
 *         +-------------+----------------+------------------+
 * 
 * 
 */
class CustomProtobufEncoder(val repo: MethodLookup) extends OneToOneEncoder {

  @throws(classOf[Exception])
  def encode(ctx: ChannelHandlerContext, channel: Channel, msg: Object): Object = {
    
    if (!msg.isInstanceOf[Tuple2[String, Message]]) {
      return msg
    }

    val t = msg.asInstanceOf[Tuple2[String, Message]]
    val methodNameCode = repo.encode(t._1)		
    val message = (t._2.asInstanceOf[Message]).toByteArray()    

    val methodNameBuf = ChannelBuffers.buffer(4)
    methodNameBuf.writeInt(methodNameCode)

    val msgLenBuf = ChannelBuffers.buffer(4)
    msgLenBuf.writeInt(message.length)
    
    ctx.getPipeline().remove(this)  
    
    ChannelBuffers.wrappedBuffer(methodNameBuf, msgLenBuf,
      ChannelBuffers.wrappedBuffer(message))
  }

}
