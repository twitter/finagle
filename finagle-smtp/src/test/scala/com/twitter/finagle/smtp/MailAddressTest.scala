package com.twitter.finagle.smtp

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import com.twitter.util.Try

@RunWith(classOf[JUnitRunner])
class MailAddressTest extends FunSuite {
  test("parse local part and domain") {
    val addressString = "local@domain.org"
    val address = MailingAddress(addressString)
    assert(address.local === "local", "local")
    assert(address.domain === "domain.org", "domain")
  }

  test("throw exception when incorrect syntax is used") {
    val incorrectAddress = "in.correct"
    val localempty = Try(MailingAddress("@domain"))
    val domainempty = Try(MailingAddress("local@"))
    val address = Try(MailingAddress(incorrectAddress))
    assert(address.isThrow, "no @")
    assert(localempty.isThrow, "local empty")
    assert(domainempty.isThrow, "domain empty")
  }

  test("mailbox") {
    val address = new MailingAddress("local", "domain.org")
    val mailbox = address.mailbox
    assert(mailbox === "local@domain.org")
  }

  test("empty") {
    val empty = MailingAddress.empty
    assert(empty.isEmpty)
  }

  test("mailboxList") {
    val list = Seq(
      MailingAddress("1@ex.com"),
      MailingAddress.empty,
      MailingAddress("2@ex.com")
    )

    val mailboxList = MailingAddress.mailboxList(list)

    assert(mailboxList === "1@ex.com,2@ex.com")
  }
}
