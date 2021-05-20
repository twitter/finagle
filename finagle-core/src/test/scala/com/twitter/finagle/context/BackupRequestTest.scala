package com.twitter.finagle.context

import org.scalatest.funsuite.AnyFunSuite

class BackupRequestTest extends AnyFunSuite {

  test("basics") {
    assert(!BackupRequest.wasInitiated)
    BackupRequest.let {
      assert(BackupRequest.wasInitiated)
    }
  }

}
