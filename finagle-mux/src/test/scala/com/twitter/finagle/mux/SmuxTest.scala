package com.twitter.finagle.mux

import com.twitter.finagle.Mux
import com.twitter.finagle.mux.exp.pushsession.MuxPush


class StandardSmuxTest extends AbstractSmuxTest {
  type ServerT = Mux.Server
  type ClientT = Mux.Client

  def serverImpl(): Mux.Server = Mux.server

  def clientImpl(): Mux.Client = Mux.client
}


class PushSmuxTest extends AbstractSmuxTest {
  type ServerT = Mux.Server
  type ClientT = MuxPush.Client

  def serverImpl(): Mux.Server = Mux.server

  def clientImpl(): MuxPush.Client = MuxPush.client
}
