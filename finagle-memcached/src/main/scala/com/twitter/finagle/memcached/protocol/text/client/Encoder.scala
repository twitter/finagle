package com.twitter.finagle.memcached.protocol.text.client

import com.twitter.finagle.memcached.protocol.text.Show
import com.twitter.finagle.memcached.protocol.Command
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}

class Encoder extends OneToOneEncoder {
  def encode(context: ChannelHandlerContext, channel: Channel, message: AnyRef) = {
    message match {
      case command: Command => Show(command)
    }
  }
}