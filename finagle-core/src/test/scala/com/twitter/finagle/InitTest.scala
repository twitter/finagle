package com.twitter.finagle

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
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
