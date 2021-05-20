package com.twitter.finagle.http

import org.scalatest.funsuite.AnyFunSuite

class ResponseTest extends AnyFunSuite {
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
