package com.twitter.finagle.mysql.unit.harness

import com.twitter.finagle.mysql.harness.config.{MySqlUserType, ROUser, RWUser}
import org.scalatest.funsuite.AnyFunSuite

class MySqlUserTest extends AnyFunSuite {
  test("RWUsers should have RW user type") {
    val user = RWUser("rw", Some("rwpassword"))

    assert(user.name == "rw")
    assert(user.password.contains("rwpassword"))
    assert(user.userType == MySqlUserType.RW)
    assert(user.userType.value == "ALL")
  }
  test("ROUsers should have RO user type") {
    val user = ROUser("ro", Some("ropassword"))

    assert(user.name == "ro")
    assert(user.password.contains("ropassword"))
    assert(user.userType == MySqlUserType.RO)
    assert(user.userType.value == "SELECT")
  }
}
