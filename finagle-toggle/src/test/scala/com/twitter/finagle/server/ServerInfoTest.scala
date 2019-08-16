package com.twitter.finagle.server

import org.scalatest.FunSuite

class ServerInfoTest extends FunSuite {

  test("ServerInfo.Empty") {
    assert(ServerInfo.Empty.environment.isEmpty)
  }

}
