package com.twitter.finagle

import org.scalatest.FunSuite

class InitTest extends FunSuite {

  test("loadBuildProperties") {
    Init.loadBuildProperties match {
      case None =>
        fail("build.properties not found for finagle-core")
      case Some(p) =>
        val version = p.getProperty("version")
        assert(version != null)
    }
  }

}
