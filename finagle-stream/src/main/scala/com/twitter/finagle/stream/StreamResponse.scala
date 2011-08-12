package com.twitter.finagle.stream

import java.io.InputStream
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBufferInputStream}
import org.jboss.netty.handler.codec.http.HttpResponse
import com.twitter.concurrent.{Channel, ChannelSource, Offer}

trait StreamResponse {
  /**
   * This represents the actual HTTP reply (response headers) for this
   * response.
   */
  def httpResponse: HttpResponse

  /**
   * An Offer for the next message in the stream.
   */
  def messages: Offer[ChannelBuffer]

  /**
   * An offer for the error.  When this enables, the stream is closed
   * (no more messages will be sent)
   */
  def error: Offer[Throwable]

  /**
   * A {{com.twitter.concurrent.Channel}} interface for the stream of
   * messages in this response.  Important: use of this interface is
   * mutually exclusive with {{messages}} and {{error}}.
   */
  final lazy val channel: Channel[ChannelBuffer] = {
    val ch = new ChannelSource[ChannelBuffer]
    messages.enumToChannel(ch)
    error foreach { _ => ch.close() }
    ch
  }

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
