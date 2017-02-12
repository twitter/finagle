package com.twitter.finagle.serverset2

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.Assertions._

class TestIdentity extends Identity {
  val scheme = "test"
  val id = Some("testdata")
  val priority = 0
}

@RunWith(classOf[JUnitRunner])
class IdentityTest extends FunSuite {
  test("UserIdentity captures current user") {
    val userIdent = new UserIdentity()
    val currentUser = System.getProperty("user.name")

    assert(userIdent.id == Some(currentUser))
    assert(userIdent.scheme == "user")
  }

  test("Identities.get() returns TestIdentity before UserIdentity") {
    val userIdent = new UserIdentity()
    val testIdent = new TestIdentity()

    assert(Identities.get() == Seq(
      "/%s/%s".format(testIdent.scheme, testIdent.id.get),
      "/%s/%s".format(userIdent.scheme, userIdent.id.get))
    )
  }
}
