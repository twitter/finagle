package com.twitter.finagle.stream

import java.io.InputStream
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBufferInputStream}
import org.jboss.netty.handler.codec.http.HttpResponse
import com.twitter.concurrent.Offer

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
