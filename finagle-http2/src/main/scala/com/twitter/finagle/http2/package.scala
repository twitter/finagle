package com.twitter.finagle

import com.twitter.finagle.Http.param.ParameterizableTransporter

package object http2 {
  private[finagle] val Http2: ParameterizableTransporter =
    ParameterizableTransporter(Http2Transporter.apply _)
}
