package com.twitter.finagle.mux

import com.twitter.finagle.Mux
import com.twitter.finagle.Mux.param.{MaxFrameSize, MuxImpl}
import com.twitter.conversions.storage._

class Netty4EndToEndTest extends AbstractEndToEndTest {
  def implName: String = "netty4"
  def clientImpl() = Mux.client.configured(MuxImpl.Netty4)
  def serverImpl() = Mux.server.configured(MuxImpl.Netty4)
}

class FragmentingNetty4EndToEndTest extends AbstractEndToEndTest {
  def implName: String = "netty4"
  def clientImpl() = Mux.client.configured(MuxImpl.Netty4).configured(MaxFrameSize(5.bytes))
  def serverImpl() = Mux.server.configured(MuxImpl.Netty4).configured(MaxFrameSize(5.bytes))
}

class Netty4RefCountingControlEndToEndTest extends AbstractEndToEndTest {
  def implName: String = "netty4"
  def clientImpl() = Mux.client.configured(MuxImpl.Netty4RefCountingControl)
  def serverImpl() = Mux.server.configured(MuxImpl.Netty4RefCountingControl)
}

class FragmentingNetty4RefCountingControlEndToEndTest extends AbstractEndToEndTest {
  def implName: String = "netty4"
  def clientImpl() =
    Mux.client.configured(MuxImpl.Netty4RefCountingControl).configured(MaxFrameSize(5.bytes))
  def serverImpl() =
    Mux.server.configured(MuxImpl.Netty4RefCountingControl).configured(MaxFrameSize(5.bytes))
}
