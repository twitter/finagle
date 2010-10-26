package com.twitter.finagle.streaming

import org.specs.Specification
import org.specs.matcher.Matcher

import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import com.twitter.finagle.SunkChannel
import com.twitter.silly.Silly

import java.io.{InputStreamReader, LineNumberReader}
import java.util.zip.GZIPInputStream

class HosebirdSpecification extends Specification {
  def makeChannel(codec: ChannelHandler) = SunkChannel {
    val pipeline = Channels.pipeline()
    pipeline.addLast("codec", codec)
    pipeline
  }
  val sampleDataResourcePath = "/hosebird-sample.json.gz"

  def sampleJSONInputStream: LineNumberReader =
    new LineNumberReader(
      new InputStreamReader(
        new GZIPInputStream(
          getClass.getResourceAsStream(sampleDataResourcePath))))
}

object HosebirdCodecSpec extends HosebirdSpecification {
  "read one item from the JSON input stream" in {
    val line = sampleJSONInputStream.readLine()
    val ch = makeChannel(new HosebirdCodec)
    ch.upstreamEvents must haveSize(0)
    Channels.fireMessageReceived(ch, line)
    ch.upstreamEvents must haveSize(1)
  }
}
