package com.twitter.finagle.thriftmux

import com.twitter.finagle.mux.exp.pushsession.MuxPush
import com.twitter.finagle.ThriftMux
import com.twitter.finagle.toggle.flag
import org.scalatest.FunSuite

class ThriftMuxTogglesTest extends FunSuite {
  // server tests
  test("When toggle disabled use the transport-based Mux server as the default muxer") {
    flag.overrides.let(ThriftMux.Server.UsePushMuxServerToggleName, 0.0) {
      assert(ThriftMux.server.muxer.isInstanceOf[ThriftMux.ServerMuxer])
    }
  }

  test("When toggle enabled use the push-based Mux server as the default muxer") {
    flag.overrides.let(ThriftMux.Server.UsePushMuxServerToggleName, 1.0) {
      assert(ThriftMux.server.muxer.isInstanceOf[MuxPush.Server])
    }
  }
}
