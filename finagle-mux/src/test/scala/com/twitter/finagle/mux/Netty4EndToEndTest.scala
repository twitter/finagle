package com.twitter.finagle.mux

import com.twitter.finagle.Mux
import com.twitter.finagle.Mux.param.{MaxFrameSize, MuxImpl}
import com.twitter.conversions.storage._
import com.twitter.finagle.mux.exp.pushsession.MuxPush

class Netty4RefCountingControlEndToEndTest extends AbstractEndToEndTest {
  override type ClientT = Mux.Client
  override type ServerT = Mux.Server
  def implName: String = "netty4 (ref counting)"

  def clientImpl() = Mux.client.configured(MuxImpl.Netty4RefCountingControl)
  def serverImpl() = Mux.server.configured(MuxImpl.Netty4RefCountingControl)
}

class FragmentingNetty4RefCountingControlEndToEndTest extends AbstractEndToEndTest {
  override type ClientT = Mux.Client
  override type ServerT = Mux.Server

  def implName: String = "netty4 (fragmenting + ref counting)"
  def clientImpl() =
    Mux.client.configured(MuxImpl.Netty4RefCountingControl).configured(MaxFrameSize(5.bytes))
  def serverImpl() =
    Mux.server.configured(MuxImpl.Netty4RefCountingControl).configured(MaxFrameSize(5.bytes))
}

class PushToStandardMuxEndToEndTest extends AbstractEndToEndTest {
  override type ClientT = MuxPush.Client
  override type ServerT = Mux.Server
  def implName: String = "push-based"
  def clientImpl() = MuxPush.client
  def serverImpl() = Mux.server
}

class StandardToPushMuxEndToEndTest extends AbstractEndToEndTest {
  override type ClientT = Mux.Client
  override type ServerT = MuxPush.Server
  def implName: String = "push-based"
  def clientImpl() = Mux.client
  def serverImpl() = MuxPush.server
}

class PushToPushMuxEndToEndTest extends AbstractEndToEndTest {
  override type ClientT = MuxPush.Client
  override type ServerT = MuxPush.Server
  def implName: String = "push-based"
  def clientImpl() = MuxPush.client
  def serverImpl() = MuxPush.server
}

class FragmentingPushMuxEndToEndTest extends AbstractEndToEndTest {
  override type ClientT = MuxPush.Client
  override type ServerT = MuxPush.Server
  def implName: String = "push-based"
  def clientImpl() = MuxPush.client.configured(MaxFrameSize(5.bytes))
  def serverImpl() = MuxPush.server.configured(MaxFrameSize(5.bytes))
}
