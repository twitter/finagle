package com.twitter.finagle.smtp

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import com.twitter.finagle.Service
import com.twitter.util.{Try, Await, Future}
import java.text.SimpleDateFormat
import java.util.Locale
import com.twitter.finagle.smtp.filter.{HeadersFilter, MailFilter, DataFilter}
import com.twitter.finagle.smtp.reply.Reply

//a reply that holds request as it is sent to the service
case class TestReply(req: Request) extends Reply {
  val code = 0
  val info = "test ok"
}

class TestError extends Error {
  val code = -1
  val info = "test error"
}

@RunWith(classOf[JUnitRunner])
class DataFilterTest extends FunSuite {

  val TestService = new Service[Request, Reply] {
    def apply(req: Request): Future[Reply] = Future { TestReply(req)}
  }
  val dataFilterService = DataFilter andThen TestService

  test("makes data end with <CRLF>.<CRLF>") {
    val data = Seq("line1", "line2.")
    val request = Request.Data(data)
    val response = Await.result(dataFilterService(request)).asInstanceOf[TestReply]
    assert(response.req.cmd === "line1\r\nline2.\r\n.") //last /r/n will be added by encoder, as after any other command
  }

  test("duplicates leading dot") {
    val data = Seq(".", ".line1", "line2.")
    val request = Request.Data(data)
    val response = Await.result(dataFilterService(request)).asInstanceOf[TestReply]
    assert(response.req.cmd === "..\r\n..line1\r\nline2.\r\n.") //last /r/n will be added by encoder, as after any other command
  }

  test("ignores non-Data commands") {
    val req1 = Request.Hello
    val req2 = Request.AddSender(MailingAddress("test@test.test"))
    val rep1 = Await.result(dataFilterService(req1)).asInstanceOf[TestReply]
    assert(rep1.req === req1)
    val rep2 = Await.result(dataFilterService(req2)).asInstanceOf[TestReply]
    assert(rep2.req === req2)
  }
}


@RunWith(classOf[JUnitRunner])
class MailFilterTest extends FunSuite {
  test("transforms email to right command sequence") {
  def MailTestService(msg: EmailMessage) = new Service[Request, Reply] {
    var cmdSeq = Seq(
      Request.Hello,
      Request.AddSender(msg.sender)) ++
      msg.to.map(Request.AddRecipient(_)) ++ Seq(
      Request.BeginData,
      Request.Data(msg.body),
      Request.Quit
    )

    def apply(req: Request): Future[Reply] = {
      assert(req === cmdSeq.head)
      if (req == cmdSeq.head) {
        cmdSeq = cmdSeq.tail
        Future { TestReply(req) }
      }
      else Future.exception(new TestError)
    }
  }

    val msg = DefaultEmail()
              .from_("from@test.com")
              .to_("to@test.com")
              .subject_("test")
              .text("body")

    val mailFilterService = MailFilter andThen MailTestService(msg)
    val test = mailFilterService(msg)
  }
}

@RunWith(classOf[JUnitRunner])
class HeaderFilterTest extends FunSuite {
  def hasOneHeader(body: Seq[String], header: String): Boolean =
    body.count(_.startsWith(header.toLowerCase)) == 1
  def checkHeader(body: Seq[String], header: String, expected: String) = {
    val line = body.find(_.startsWith(header.toLowerCase))
    line match {
      case Some(hdr) => {
        val value = hdr drop header.length
        assert(value === expected, "%s is incorrect" format header)
      }
      case None => throw new TestError
    }
  }

  val simpleMsg = DefaultEmail()
    .from_("from@test.com")
    .to_("to@test.com")
    .subject_("test")
    .text("body")

  val multipleAddressMsg = DefaultEmail()
    .from_("from1@test.com", "from2@test.com")
    .to_("to1@test.com", "to2@test.com")
    .subject_("test")
    .text("body")


  test("result message has all necessary headers") {
    val headerTestService = new Service[EmailMessage, Unit] {
      def apply(msg: EmailMessage): Future[Unit] = Future {
        val body = msg.body
        val headers = Seq("From: ", "To: ", "Subject: ", "Date: ")
        val hasHeaders = headers.map(hasOneHeader(body, _)).reduce(_ && _)
        assert(hasHeaders)
      }
    }

    val headerFilterService = HeadersFilter andThen headerTestService
    val test = headerFilterService(simpleMsg)
  }

  test("message has Sender header in case of multiple From") {
    val headerTestService = new Service[EmailMessage, Unit] {
      def apply(msg: EmailMessage): Future[Unit] = {
        val body = msg.body
        if (msg.from.length > 1) Future  {
          assert(hasOneHeader(body, "Sender: "))
          checkHeader(body, "Sender: ", msg.sender.mailbox)
        }
        else Future.Done
      }
    }

    val headerFilterService = HeadersFilter andThen headerTestService
    val test = Await result headerFilterService(multipleAddressMsg)
  }

  test("necessary headers correspond to the right values") {
    val headerTestService = new Service[EmailMessage, Unit] {
      def apply(msg: EmailMessage): Future[Unit] = Future {
        val body = msg.body
        checkHeader(body, "From: ", msg.from.map(_.mailbox).mkString(","))
        checkHeader(body, "To: ", msg.to.map(_.mailbox).mkString(","))
        checkHeader(body, "Subject: ", msg.subject)
        checkHeader(body, "Date: ", EmailMessage.DateFormat.format(msg.date))
      }
    }

    val headerFilterService = HeadersFilter andThen headerTestService
    val test = headerFilterService(multipleAddressMsg)
  }
}