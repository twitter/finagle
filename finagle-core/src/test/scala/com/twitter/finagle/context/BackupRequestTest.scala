package com.twitter.finagle.context

import org.scalatest.FunSuite

class BackupRequestTest extends FunSuite {

  test("basics") {
    assert(!BackupRequest.wasInitiated)
    BackupRequest.let {
      assert(BackupRequest.wasInitiated)
    }
  }

}
