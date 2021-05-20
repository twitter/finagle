package com.twitter.finagle.serverset2

import org.scalatest.funsuite.AnyFunSuite

class TestIdentity extends Identity {
  val scheme = "test"
  val id = Some("testdata")
  val priority = 0
}

class IdentityTest extends AnyFunSuite {
  test("UserIdentity captures current user") {
    val userIdent = new UserIdentity()
    val currentUser = System.getProperty("user.name")

    assert(userIdent.id == Some(currentUser))
    assert(userIdent.scheme == "user")
  }

  test("Identities.get() returns TestIdentity before UserIdentity") {
    val userIdent = new UserIdentity()
    val testIdent = new TestIdentity()

    assert(
      Identities
        .get()
        .filter(identity =>
          (identity.startsWith("/%s/".format(testIdent.scheme)) ||
            identity.startsWith("/%s/".format(userIdent.scheme)))) == Seq(
        "/%s/%s".format(testIdent.scheme, testIdent.id.get),
        "/%s/%s".format(userIdent.scheme, userIdent.id.get)
      )
    )
  }
}
