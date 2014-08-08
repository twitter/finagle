package com.twitter.finagle.smtp

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EmailBuilderTest extends FunSuite {
  val defaultBuilder = EmailBuilder()

  val testFrom = "from@from"
  val testTo = "to@to"
  val testCc = "cc@cc"
  val testBcc = "bcc@bcc"
  val testReplyTo = "reply@to"

  def mailseq(list: String*): Seq[MailingAddress] = list.seq.map(MailingAddress(_))

  test("from") {

    val addfrom = defaultBuilder.from_("from@from.com").from_("from2@from.com")
    assert(addfrom.from.map(_.mailbox) === Seq("from@from.com", "from2@from.com"), "add from")
    val setfrom = addfrom.setFrom(Seq(MailingAddress("from3@from.com")))
    assert(setfrom.from.map(_.mailbox) === Seq("from3@from.com"), "set from")
  }

  test("to") {
    val addto = defaultBuilder.to_("to@to.com").to_("to2@to.com")
    assert(addto.to.map(_.mailbox) === Seq("to@to.com", "to2@to.com"), "add to")
    val setto = addto.setTo(Seq(MailingAddress("to3@to.com")))
    assert(setto.to.map(_.mailbox) === Seq("to3@to.com"), "set to")
  }

  test("cc") {
    val addcc = defaultBuilder.cc_("cc@cc.com").cc_("cc2@cc.com")
    assert(addcc.cc.map(_.mailbox) === Seq("cc@cc.com", "cc2@cc.com"), "add cc")
    val setcc = addcc.setCc(Seq(MailingAddress("cc3@cc.com")))
    assert(setcc.cc.map(_.mailbox) === Seq("cc3@cc.com"), "set cc")
  }

  test("bcc") {
    val addbcc = defaultBuilder.bcc_("bcc@bcc.com").bcc_("bcc2@bcc.com")
    assert(addbcc.bcc.map(_.mailbox) === Seq("bcc@bcc.com", "bcc2@bcc.com"), "add bcc")
    val setbcc = addbcc.setBcc(Seq(MailingAddress("bcc3@bcc.com")))
    assert(setbcc.bcc.map(_.mailbox) === Seq("bcc3@bcc.com"), "set bcc")
  }

  test("reply-to") {
    val addreplyto = defaultBuilder.replyTo_("reply@to.com").replyTo_("reply2@to.com")
    assert(addreplyto.replyTo.map(_.mailbox) === Seq("reply@to.com", "reply2@to.com"), "add reply-to")
    val setreplyto = addreplyto.setReplyTo(Seq(MailingAddress("reply3@to.com")))
    assert(setreplyto.replyTo.map(_.mailbox) === Seq("reply3@to.com"), "set reply-to")
  }

  test("body") {
    val addlines = defaultBuilder.addBodyLines("line1").addBodyLines("line2")
    assert(addlines.body === Seq("line1", "line2"), "add lines to body")
    val setlines = addlines.setBodyLines(Seq("line3"))
    assert(setlines.body === Seq("line3"), "set lines of body")
  }

  test("sender") {
    val setsender = defaultBuilder.sender_("sender@sender.com")
    assert(setsender.sender.mailbox === "sender@sender.com", "add sender")
    assert(setsender.from.map(_.mailbox) === Seq("sender@sender.com"), "add sender to from in case it is the first")
  }
}
