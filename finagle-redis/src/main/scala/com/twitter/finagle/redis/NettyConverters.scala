package com.twitter.finagle.redis

import com.twitter.finagle.netty3.{BufChannelBuffer, ChannelBufferBuf}
import com.twitter.io.Buf
import org.jboss.netty.buffer.ChannelBuffer
import scala.language.implicitConversions

/**
 * A temporary solution to migrate user's code off of Netty types.
 */
object NettyConverters {

  implicit class BufAsNetty(val buf: Buf) extends AnyVal {
    def asNetty: ChannelBuffer = BufChannelBuffer(buf)
  }

  implicit class BufTupleAsNetty(val buf: (Buf, Buf)) extends AnyVal {
    def asNetty: (ChannelBuffer, ChannelBuffer) =
      BufChannelBuffer(buf._1) -> BufChannelBuffer(buf._2)
  }

  implicit class ChannelBufferAsFinagle(val buf: ChannelBuffer) extends AnyVal {
    def asFinagle: Buf = ChannelBufferBuf.Owned(buf)
  }

  implicit class ChannelBufferTupleAsFinagle(val buf: (ChannelBuffer, ChannelBuffer)) extends AnyVal {
    def asFinagle: (Buf, Buf) = ChannelBufferBuf.Owned(buf._1) -> ChannelBufferBuf.Owned(buf._2)
  }
}

