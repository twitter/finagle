package com.twitter.finagle.smtp

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import com.twitter.finagle.Service
import com.twitter.util.{Await, Future}
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
    def apply(req: Request): Future[Reply] = Future { new TestReply(req)}
  }
  val dataFilterService = DataFilter andThen TestService

  test("makes data end with <CRLF>.<CRLF>") {
    val data = Seq("line1", "line2.")
    val request = Data(data)
    val response = Await.result(dataFilterService(request)).asInstanceOf[TestReply]
    assert(response.req.cmd === "line1\r\nline2.\r\n.") //last /r/n will be added by encoder, as after any other command
  }

  test("duplicates leading dot") {
    val data = Seq(".", ".line1", "line2.")
    val request = Data(data)
    val response = Await.result(dataFilterService(request)).asInstanceOf[TestReply]
    assert(response.req.cmd === "..\r\n..line1\r\nline2.\r\n.") //last /r/n will be added by encoder, as after any other command
  }

  test("ignores non-Data commands") {
    val req1 = Request.Hello
    val req2 = AddFrom(MailingAddress("test@test.test"))
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
      AddFrom(msg.getSender)) ++
      msg.getTo.map(AddRecipient(_)) ++ Seq(
      Request.BeginData,
      Data(msg.getBody),
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

    val msg = EmailMessage("from@test.com", "to@test.com", "test", Seq("body"))
    val mailFilterService = MailFilter andThen MailTestService(msg)
    val test = mailFilterService(msg)
  }
}

@RunWith(classOf[JUnitRunner])
class HeaderFilterTest extends FunSuite {
  def hasOneHeader(body: Seq[String], header: String): Boolean = body.count(_.startsWith(header)) == 1
  def checkHeader(body: Seq[String], header: String, expected: String) = {
    val line = body.find(_.startsWith(header))
    line match {
      case Some(hdr) => {
        val value = hdr drop header.length
        assert(value === expected, header)
      }
      case None => throw new TestError
    }
  }

  val simpleMsg = EmailMessage("from@test.com", "to@test.com", "test", Seq("body"))
  val multipleAddressMsg = EmailMessage(Seq(MailingAddress("from1@test.com"),MailingAddress("from2@test.com")),
    Seq(MailingAddress("to1@test.com"), MailingAddress("to2@test.com")),
    "test", Seq("body"))

  test("result message has all necessary headers") {
    val headerTestService = new Service[EmailMessage, Unit] {
      def apply(msg: EmailMessage): Future[Unit] = Future {
        val body = msg.getBody
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
        val body = msg.getBody
        if (msg.getFrom.length > 1) Future {
          assert(hasOneHeader(body, "Sender: "))
          checkHeader(body, "Sender: ", msg.getSender.toString)
        }
        else Future.Done
      }
    }

    val headerFilterService = HeadersFilter andThen headerTestService
    val test = headerFilterService(multipleAddressMsg)
  }

  test("necessary headers correspond to the right values") {
    val headerTestService = new Service[EmailMessage, Unit] {
      def apply(msg: EmailMessage): Future[Unit] = Future {
        val body = msg.getBody
        checkHeader(body, "From: ", msg.getFrom.mkString(","))
        checkHeader(body, "To: ", msg.getTo.mkString(","))
        checkHeader(body, "Subject: ", msg.getSubject)
        checkHeader(body, "Date: ", new SimpleDateFormat("EE, dd MMM yyyy HH:mm:ss ZZ", Locale.forLanguageTag("eng")).format(msg.getDate))
      }
    }

    val headerFilterService = HeadersFilter andThen headerTestService
    val test = headerFilterService(multipleAddressMsg)
  }
}