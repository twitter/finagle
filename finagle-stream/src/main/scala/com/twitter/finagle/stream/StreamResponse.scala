package com.twitter.finagle.stream

import com.twitter.concurrent.Channel
import java.io.InputStream
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBufferInputStream}
import org.jboss.netty.handler.codec.http.HttpResponse

trait StreamResponse {
  def httpResponse: HttpResponse

  /**
   * As data arrives, it is published via this channel.
   */
  def channel: Channel[ChannelBuffer]

  /**
   * Rather than reacting to channel updates, you can read the content
   * in a more traditional way via an InputStream.
   */
  lazy val inputStream: InputStream = new ChannelBufferInputStream(httpResponse.getContent)

  /**
   * If you are done reading the response, you can call release() to
   * abort further processing of the response.
   */
  def release()
}
