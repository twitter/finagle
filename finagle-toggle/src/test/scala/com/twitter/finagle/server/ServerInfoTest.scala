package com.twitter.finagle.server

import org.scalatest.funsuite.AnyFunSuite

class ServerInfoTest extends AnyFunSuite {

  test("ServerInfo.Empty") {
    assert(ServerInfo.Empty.environment.isEmpty)
  }

}
