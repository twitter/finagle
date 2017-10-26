package com.twitter.finagle.netty4

import com.twitter.finagle.stats.InMemoryStatsReceiver
import io.netty.channel.ChannelPipeline
import io.netty.handler.codec.{DelimiterBasedFrameDecoder, Delimiters}
import io.netty.handler.codec.string.{StringDecoder, StringEncoder}
import java.nio.charset.StandardCharsets.UTF_8

private object Netty4ListenerHelpers {

  object StringServerInit extends (ChannelPipeline => Unit) {
    def apply(pipeline: ChannelPipeline): Unit = {
      pipeline.addLast("line", new DelimiterBasedFrameDecoder(100, Delimiters.lineDelimiter(): _*))
      pipeline.addLast("stringDecoder", new StringDecoder(UTF_8))
      pipeline.addLast("stringEncoder", new StringEncoder(UTF_8))
    }
  }

  class StatsCtx {
    val sr: InMemoryStatsReceiver = new InMemoryStatsReceiver

    def statEquals(name: String*)(expected: Float*): Unit =
      assert(sr.stat(name: _*)() == expected)

    def counterEquals(name: String*)(expected: Int): Unit =
      assert(sr.counters(name) == expected)
  }
}
