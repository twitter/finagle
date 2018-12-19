package com.twitter.finagle.mux

import com.twitter.finagle.Mux.param.MaxFrameSize
import com.twitter.conversions.StorageUnitOps._
import com.twitter.finagle.Mux

class PushToPushMuxEndToEndTest extends AbstractEndToEndTest {
  override type ClientT = Mux.Client
  override type ServerT = Mux.Server
  def implName: String = "push-based"
  def clientImpl() = Mux.client
  def serverImpl() = Mux.server
}

class FragmentingPushMuxEndToEndTest extends AbstractEndToEndTest {
  override type ClientT = Mux.Client
  override type ServerT = Mux.Server
  def implName: String = "push-based"
  def clientImpl() = Mux.client.configured(MaxFrameSize(5.bytes))
  def serverImpl() = Mux.server.configured(MaxFrameSize(5.bytes))
}
