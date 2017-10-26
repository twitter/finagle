package com.twitter.finagle.mux

import com.twitter.finagle.Mux
import com.twitter.finagle.Mux.param.{MaxFrameSize, MuxImpl}
import com.twitter.conversions.storage._
import com.twitter.finagle.mux.exp.pushsession.MuxPush

class Netty4RefCountingControlEndToEndTest extends AbstractEndToEndTest {
  override type ClientT = Mux.Client
  def implName: String = "netty4 (ref counting)"

  def clientImpl() = Mux.client.configured(MuxImpl.Netty4RefCountingControl)
  def serverImpl() = Mux.server.configured(MuxImpl.Netty4RefCountingControl)
}

class FragmentingNetty4RefCountingControlEndToEndTest extends AbstractEndToEndTest {

  override type ClientT = Mux.Client
  def implName: String = "netty4 (fragmenting + ref counting)"
  def clientImpl() =
    Mux.client.configured(MuxImpl.Netty4RefCountingControl).configured(MaxFrameSize(5.bytes))
  def serverImpl() =
    Mux.server.configured(MuxImpl.Netty4RefCountingControl).configured(MaxFrameSize(5.bytes))
}

class PushMuxEndToEndTest extends AbstractEndToEndTest {
  override type ClientT = MuxPush.Client
  def implName: String = "push-based"
  def clientImpl() = MuxPush.client
  def serverImpl() = Mux.server
}
