package com.twitter.finagle

import com.twitter.finagle.Stack.Params
import com.twitter.finagle.param.ProtocolLibrary
import com.twitter.finagle.netty4.http.{Netty4ClientStreamTransport, Netty4ServerStreamTransport}

package object http2 {
  val Http2: Params = Params.empty + Http.HttpImpl(
    new Netty4ClientStreamTransport(_),
    new Netty4ServerStreamTransport(_),
    Http2Transporter.apply _,
    Http2Listener.apply _
  ) + ProtocolLibrary("http/2")
}
