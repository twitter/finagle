package com.twitter.finagle.http.exp

import com.twitter.finagle.http.{Request, RequestConfig}

/** Abstract representation of an encoder for a HTTP Post request */
private[finagle] trait FormPostEncoder {

  /** Create a new [[Request]] which represents the current state of the config */
  def encode(config: RequestConfig, multipart: Boolean): Request
}
