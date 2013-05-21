package com.twitter.finagle.stream

import com.twitter.conversions.time._
import com.twitter.finagle.{SunkChannel, SunkChannelFactory}
import com.twitter.util.Await
import java.nio.charset.Charset
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.{Channels, MessageEvent}
import org.jboss.netty.handler.codec.http._
import org.specs.SpecificationWithJUnit

class HttpDechunkerSpec extends SpecificationWithJUnit {

  "HttpDechunker" should {
    "wait until last message is synced before sending EOF" in {
      val cf = new SunkChannelFactory
      val pipeline = Channels.pipeline
      val dechunker = new HttpDechunker
      pipeline.addLast("dechunker", dechunker)
      val channel = new SunkChannel(cf, pipeline, cf.sink)

      val httpResponse = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
      httpResponse.setChunked(true)

      Channels.fireMessageReceived(channel, httpResponse)

      val streamResponse = (channel.upstreamEvents.headOption match {
        case Some(m: MessageEvent) => m.getMessage match {
          case s: StreamResponse => s
        }
        case _ => throw new Exception("No upstream message received")
      })

      val messages = streamResponse.messages
      val error = streamResponse.error

      Channels.fireMessageReceived(channel, new DefaultHttpChunk(ChannelBuffers.wrappedBuffer("1".getBytes)))
      Await.result(messages.sync(), 1.second).toString(Charset.defaultCharset) must_== "1"

      Channels.fireMessageReceived(channel, new DefaultHttpChunk(ChannelBuffers.wrappedBuffer("2".getBytes)))
      Channels.fireMessageReceived(channel, HttpChunk.LAST_CHUNK)
      val receiveError = error.sync()
      receiveError.isDefined must_== false
      Await.result(messages.sync(), 1.second).toString(Charset.defaultCharset) must_== "2"
      receiveError.isDefined must_== true
      Await.result(receiveError, 1.second) must_== EOF
    }
  }
}
