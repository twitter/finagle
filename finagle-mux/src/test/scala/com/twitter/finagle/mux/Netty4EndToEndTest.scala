package com.twitter.finagle.mux

import com.twitter.finagle.Mux
import com.twitter.finagle.Mux.param.MuxImpl

class Netty4EndToEndTest extends AbstractEndToEndTest {
  def implName: String = "netty4"
  def clientImpl(): MuxImpl = Mux.param.MuxImpl.Netty4
  def serverImpl(): MuxImpl = Mux.param.MuxImpl.Netty4
}

class Netty4RefCountingControlEndToEndTest extends AbstractEndToEndTest {
  def implName: String = "netty4"
  def clientImpl(): MuxImpl = Mux.param.MuxImpl.Netty4RefCountingControl
  def serverImpl(): MuxImpl = Mux.param.MuxImpl.Netty4RefCountingControl
}
