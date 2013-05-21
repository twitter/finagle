package com.twitter.finagle.example.stream

import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.stream.{Stream, StreamResponse}
import com.twitter.conversions.time._
import com.twitter.util.{Base64StringEncoder => Base64, Future}
import org.jboss.netty.handler.codec.http.{HttpRequest, HttpVersion, HttpMethod, DefaultHttpRequest}
import org.jboss.netty.util.CharsetUtil

/**
 * This client connects to a Streaming HTTP service, prints 1000 messages, then disconnects.
 * If you start two or more of these clients simultaneously, you will notice that this
 * is also a PubSub example.
 */
object HosebirdClient {
  def main(args: Array[String]) {
    val username = args(0)
    val password = args(1)
    val hostAndPort = args(2)
    val path = args(3)

    // Construct a ServiceFactory rather than a Client since the TCP Connection
    // is stateful (i.e., messages on the stream even after the initial response).
    val clientFactory: ServiceFactory[HttpRequest, StreamResponse] = ClientBuilder()
      .codec(Stream())
      .hosts(hostAndPort)
      .tcpConnectTimeout(1.microsecond)
      .hostConnectionLimit(1)
      .buildFactory()

    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, path)
    val userpass = username + ":" + password
    request.setHeader("Authorization", "Basic " + Base64.encode(userpass.getBytes("UTF-8")))
    request.setHeader("User-Agent", "Finagle 0.0")
    request.setHeader("Host", hostAndPort)
    println(request)
    for {
      client <- clientFactory()
      streamResponse <- client(request)
    } {
      val httpResponse = streamResponse.httpResponse
      if (httpResponse.getStatus.getCode != 200) {
        println(httpResponse.toString)
        client.close()
        clientFactory.close()
      } else {
        var messageCount = 0 // Wait for 1000 messages then shut down.
        streamResponse.messages foreach { buffer =>
          messageCount += 1
          println(buffer.toString(CharsetUtil.UTF_8))
          println("--")
          if (messageCount == 1000) {
            client.close()
            clientFactory.close()
          }
          // We return a Future indicating when we've completed processing the message.
          Future.Done
        }
      }
    }
  }
}
