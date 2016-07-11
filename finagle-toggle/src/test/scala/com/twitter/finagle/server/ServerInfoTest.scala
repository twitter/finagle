package com.twitter.finagle.server

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ServerInfoTest extends FunSuite {

  test("ServerInfo.Empty") {
    assert(ServerInfo.Empty.environment.isEmpty)
  }

  test("ServerInfo.Flag") {
    environment.let("staging") {
      assert(ServerInfo.Flag.environment.contains("staging"))
    }
    environment.let("production") {
      assert(ServerInfo.Flag.environment.contains("production"))
    }
  }

}
