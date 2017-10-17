package com.twitter.finagle.thriftmux

import com.twitter.finagle.mux.exp.pushsession.MuxPush
import com.twitter.finagle.{Mux, ThriftMux}
import com.twitter.finagle.toggle.flag
import org.scalatest.FunSuite

class ThriftMuxTogglesTest extends FunSuite {

  test("When toggle disabled use the transport-based Mux client as the default muxer") {
    flag.overrides.let(ThriftMux.Client.UsePushMuxToggleName, 0.0) {
      ThriftMux.client.muxer.isInstanceOf[Mux.Client]
    }
  }

  test("When toggle enabled use the push-based Mux client as the default muxer") {
    flag.overrides.let(ThriftMux.Client.UsePushMuxToggleName, 1.0) {
      ThriftMux.client.muxer.isInstanceOf[MuxPush.Client]
    }
  }
}
