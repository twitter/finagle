package com.twitter.finagle.stream

import org.specs.Specification
import com.twitter.finagle.builder.ClientBuilder
import org.jboss.netty.util.CharsetUtil
import com.twitter.finagle.kestrel.{Client => Kestrel}
import com.twitter.finagle.Service
import org.jboss.netty.handler.codec.http._
import com.twitter.concurrent.Channel
import org.jboss.netty.buffer.ChannelBuffer
import java.net.URI
import com.twitter.util.Future

object Hose {
  def apply(uriString: String, headers: (String, String)*): Channel[ChannelBuffer] = {
    val uri = new URI(uriString)
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/1/statuses/sample.json")
    headers foreach { case (key, value) =>
      request.addHeader(key, value)
    }
    request.addHeader("Host", uri.getHost)
    val port = if (uri.getPort < 0) 80 else uri.getPort
    val service = ClientBuilder()
      .codec(new Stream)
      .hosts(uri.getHost + ":" + port)
      .build()
    service(request)()
  }
}

object Stream2Kestrel extends Specification {
  "Stream Piped to Kestrel" should {
    "make you wet your pants" in {
      val stream = ClientBuilder()
        .codec(new Stream)
        .hosts("stream.twitter.com:80")
        .build()
      val hose = Hose(
        "http://stream.twitter.com/1/statuses/sample.json",
        "Authorization" -> "Basic cGl2b3Q0OnBpdm90czJwYW5kYXM=")
      val kestrel = Kestrel("localhost:22133")

      hose.pipe(kestrel.to("tweets"))
      kestrel.from("tweets").pipe(kestrel.to("tweets2"))
    }
  }
}
