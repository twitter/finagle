package com.twitter.finagle

import java.net.{UnknownHostException, InetAddress}

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TravisTest extends FunSuite {
  test("test travis CI") {
    val localAddr = InetAddress.getLocalHost()
    assert(localAddr.getHostName() != "")
  }
}
