package com.twitter.finagle.http

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ResponseTest extends FunSuite {
  test("constructors") {
    List(
      Response(),
      Response(Version.Http11, Status.Ok),
      Response()
    ).foreach { response =>
      assert(response.version == Version.Http11)
      assert(response.status == Status.Ok)
    }
  }
}
