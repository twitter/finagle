package com.twitter.finagle.streaming

import org.specs.Specification
import org.specs.matcher.Matcher

import org.jboss.netty.channel._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.http.DefaultHttpChunk
import com.twitter.finagle.SunkChannel
import com.twitter.silly.Silly

import java.io.{InputStreamReader, LineNumberReader}
import java.util.zip.GZIPInputStream

class StreamingSpecification extends Specification {
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

object StreamingCodecSpec extends StreamingSpecification {
  "read one item from the JSON input stream" in {
    val line = sampleJSONInputStream.readLine()
    val ch = makeChannel(new StreamingCodec)
    val buf = ChannelBuffers.wrappedBuffer(line.getBytes)
    val chunk = new DefaultHttpChunk(buf)
    ch.upstreamEvents must haveSize(0)
    Channels.fireMessageReceived(ch, chunk)
    ch.upstreamEvents must haveSize(1)
    val m = ch.upstreamEvents(0).asInstanceOf[MessageEvent].getMessage
    m must haveClass[CachedMessage]
    m.asInstanceOf[CachedMessage].kind mustEqual CachedMessage.KIND_STATUS
  }
}
