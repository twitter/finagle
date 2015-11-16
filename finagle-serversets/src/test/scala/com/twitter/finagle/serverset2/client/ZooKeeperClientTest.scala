package com.twitter.finagle.serverset2.client

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class ZooKeeperClientTest extends FunSuite {
  test("ZooKeeperReader.patToPathAndPrefix") {
    import ZooKeeperReader.{patToPathAndPrefix=>p}

    intercept[IllegalArgumentException] { p("") }
    intercept[IllegalArgumentException] { p("foo/bar") }

    assert(p("/") == ("/", ""))
    assert(p("/foo") == ("/", "foo"))
    assert(p("/foo/bar") == ("/foo", "bar"))
    assert(p("/foo/bar/") == ("/foo/bar", ""))
  }
}
