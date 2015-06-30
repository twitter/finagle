package com.twitter.finagle.example.stream

import com.twitter.conversions.time._
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.stream.{Header, Stream, StreamRequest, StreamResponse, Version}
import com.twitter.io.Buf
import com.twitter.util.{Base64StringEncoder => Base64, Future}

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
    val clientFactory: ServiceFactory[StreamRequest, StreamResponse] = ClientBuilder()
      .codec(Stream())
      .hosts(hostAndPort)
      .tcpConnectTimeout(1.microsecond)
      .hostConnectionLimit(1)
      .buildFactory()

    val userpass = username + ":" + password
    val request = StreamRequest(StreamRequest.Method.Get, path, headers = Seq(
      Header("Authorization", "Basic " + Base64.encode(userpass.getBytes("UTF-8"))), 
      Header("User-Agent", "Finagle 0.0"),
      Header("Host", hostAndPort)))
    println(request)
    for {
      client <- clientFactory()
      streamResponse <- client(request)
    } {
      val info = streamResponse.info
      if (info.status.code != 200) {
        println(info)
        client.close()
        clientFactory.close()
      } else {
        var messageCount = 0 // Wait for 1000 messages then shut down.
        streamResponse.messages foreach { case Buf.Utf8(str) =>
          messageCount += 1
          println(str)
          println("--")
          if (messageCount == 1000) {
            client.close()
            clientFactory.close()
          }
        }
      }
    }
  }
}
