package com.twitter.finagle.stream

import com.twitter.concurrent.Offer
import com.twitter.io.Buf

trait StreamResponse { self =>
  /**
   * This represents the actual HTTP reply (response headers) for this
   * response.
   */
  def info: StreamResponse.Info

  /**
   * An Offer for the next message in the stream.
   */
  def messages: Offer[Buf]

  /**
   * An offer for the error.  When this enables, the stream is closed
   * (no more messages will be sent)
   */
  def error: Offer[Throwable]

  /**
   * If you are done reading the response, you can call release() to
   * abort further processing of the response.
   */
  def release(): Unit
}

object StreamResponse {
  /**
   * HTTP status code.
   */
  case class Status(code: Int) {
    require(100 <= code && code <= 505)
  }
  
  /**
   * Represents structural parts of the HTTP response.
   */
  case class Info(version: Version, status: Status, headers: Seq[Header])
}
