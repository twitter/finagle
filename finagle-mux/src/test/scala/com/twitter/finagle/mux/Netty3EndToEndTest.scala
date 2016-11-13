package com.twitter.finagle.mux

import com.twitter.finagle.Mux
import com.twitter.finagle.Mux.param.MuxImpl

class Netty3EndToEndTest extends AbstractEndToEndTest {
  def implName: String = "netty3"
  def clientImpl(): MuxImpl = Mux.param.MuxImpl.Netty3
  def serverImpl(): MuxImpl = Mux.param.MuxImpl.Netty3
}