package com.twitter.finagle.thriftmux

import com.twitter.finagle.mux.exp.pushsession.MuxPush
import com.twitter.finagle.{Mux, ThriftMux}
import com.twitter.finagle.toggle.flag
import org.scalatest.FunSuite

class ThriftMuxTogglesTest extends FunSuite {

  // client tests
  test("When toggle disabled use the transport-based Mux client as the default muxer") {
    flag.overrides.let(ThriftMux.Client.UsePushMuxClientToggleName, 0.0) {
      assert(ThriftMux.client.muxer.isInstanceOf[Mux.Client])
    }
  }

  test("When toggle enabled use the push-based Mux client as the default muxer") {
    flag.overrides.let(ThriftMux.Client.UsePushMuxClientToggleName, 1.0) {
      assert(ThriftMux.client.muxer.isInstanceOf[MuxPush.Client])
    }
  }

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
