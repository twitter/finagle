package com.twitter.finagle.mux

import com.twitter.finagle.Mux

class SmuxTest extends AbstractSmuxTest {
  type ServerT = Mux.Server
  type ClientT = Mux.Client

  def serverImpl(): Mux.Server = Mux.server

  def clientImpl(): Mux.Client = Mux.client
}
