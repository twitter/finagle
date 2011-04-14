package com.twitter.finagle.example.stream

import com.twitter.concurrent.{Channel, Observer}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.stream.{Stream, StreamResponse}
import com.twitter.util.Future
import java.net.InetSocketAddress
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpVersion, HttpMethod, DefaultHttpRequest}
import org.jboss.netty.util.CharsetUtil

/**
 * This client connects to a Streaming HTTP service, prints 1000 messages, then disconnects.
 * If you start two or more of these clients simultaneously, you will notice that this
 * is also a PubSub example.
 */
object StreamClient {
  def main(args: Array[String]) {
    // Construct a ServiceFactory rather than a Client since the TCP Connection
    // is stateful (i.e., messages on the stream even after the initial response).
    val clientFactory: ServiceFactory[HttpRequest, StreamResponse] = ClientBuilder()
      .codec(Stream)
      .hosts(new InetSocketAddress(8080))
      .buildFactory()

    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/")
    for {
      client <- clientFactory.make()
      StreamResponse(httpResponse, channel) <- client(request)
    } {
      var observer: Observer = null
      var messageCount = 0 // Wait for 1000 messages then shut down.
      observer = channel.respond { buffer =>
        messageCount += 1
        println("Received message: " + buffer.toString(CharsetUtil.UTF_8))
        if (messageCount == 1000) {
          observer.dispose()
          client.release()
          clientFactory.close()
        }
        // We return a Future indicating when we've completed processing the message.
        Future.Done
      }
    }
  }
}