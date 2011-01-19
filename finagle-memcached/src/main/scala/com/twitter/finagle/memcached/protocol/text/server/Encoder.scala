package com.twitter.finagle.memcached.protocol.text.server

import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import com.twitter.finagle.memcached.protocol.text.Show
import com.twitter.finagle.memcached.protocol.Response

class Encoder extends OneToOneEncoder {
  def encode(context: ChannelHandlerContext, channel: Channel, message: AnyRef) = {
    message match {
      case response: Response => Show(response)
    }
  }
}