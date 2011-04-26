package com.twitter.finagle.example.spritzer2kestrel

import com.twitter.concurrent.{Channel, ChannelSource}
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.kestrel.{Client => Kestrel}
import com.twitter.finagle.stream.{Stream, StreamResponse}
import java.net.{Socket, ConnectException}
import java.nio.charset.Charset
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers.copiedBuffer
import org.jboss.netty.handler.codec.base64.Base64
import org.jboss.netty.handler.codec.http.DefaultHttpRequest
import org.jboss.netty.handler.codec.http.HttpMethod.GET
import org.jboss.netty.handler.codec.http.HttpVersion.HTTP_1_1
import org.jboss.netty.util.CharsetUtil

/**
 * This example demonstrates some of the composability of Channels. The Twitter
 * "Spritzer" (about 1% of all public Tweets) is represented as a readable Channel.
 *
 * Similarly, a Kestrel queue is represented as a writable Channel. The output
 * from the Spritzer is made input (piped()) to the Kestrel queue.
 */
object Spritzer2Kestrel {
  def main(args: Array[String]) {
    assertKestrelRunning()

    val userPass = args(0) // expected format: "user:password"
    val encodedUserPass =
      Base64.encode(copiedBuffer(userPass, Charset.defaultCharset)).toString(CharsetUtil.ISO_8859_1)

    // Connect to the Spritzer:
    val spritzerClient = ClientBuilder()
      .codec(Stream)
      .hosts("stream.twitter.com:80")
      .build()
    val streamResponse = {
      val request = {
        val request = new DefaultHttpRequest(HTTP_1_1, GET, "/1/statuses/sample.json")
        request.addHeader("Host", "stream.twitter.com")
        request.addHeader("Authorization", "Basic " + encodedUserPass)
        request
      }
      try {
        spritzerClient(request)()
      } catch {
        case e =>
          spritzerClient.release()
          throw e
      }
    }

    val spritzer = streamResponse.channel

    // Grab a writeable Channel connected to the Kestrel
    // queue named "queue"
    val kestrelClient = Kestrel("localhost:22133")
    val queue: ChannelSource[ChannelBuffer] = {
      kestrelClient.to("queue")
    }

    // Read from the Spritzer and write into Kestrel:
    spritzer.pipe(queue)

    // Release any resources if something goes wrong:
    (spritzer.closes or queue.closes) ensure {
      println("Connection to the Stream or Kestrel closed")
      kestrelClient.close()
      spritzerClient.release()
    }
  }

  private[this] def assertKestrelRunning() {
    try {
      new Socket("localhost", 22133)
    } catch {
      case e: ConnectException =>
        println("Error: Kestrel must be running on port 22133")
        System.exit(1)
    }
  }
}