package com.twitter.finagle.dispatch

import com.twitter.finagle.transport.Transport
import com.twitter.finagle.WriteException
import com.twitter.util.{Return, Throw, Promise, Future}
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class ClientDispatcherSpec extends SpecificationWithJUnit with Mockito {
  "ClientDispatcher" should {
    val trans = mock[Transport[String, String]]
    val disp = new SerialClientDispatcher[String, String](trans)

    "dispatch requests" in {
      trans.write("one") returns Future.value(())
      val p = new Promise[String]
      trans.read() returns p
      val f = disp("one")
      there was one(trans).write("one")
      there was one(trans).read()

      f.isDefined must beFalse
      p.setValue("ok: one")
      f.poll must beSome(Return("ok: one"))
    }

    "dispatch requests one-at-a-time" in {
      trans.write(any) returns Future.value(())
      val p0, p1 = new Promise[String]
      trans.read() returns p0
      val f0 = disp("one")
      there was one(trans).write(any)
      there was one(trans).read()
      val f1 = disp("two")
      there was one(trans).write(any)
      there was one(trans).read()

      f0.isDefined must beFalse
      f1.isDefined must beFalse

      trans.read() returns p1
      p0.setValue("ok: one")
      f0.poll must beSome(Return("ok: one"))
      there were two(trans).write(any)
      there were two(trans).read()

      f1.isDefined must beFalse
      p1.setValue("ok: two")
      p1.poll must beSome(Return("ok: two"))
    }

    "interruption" in {
      trans.write(any) returns Future.value(())
      val p0 = new Promise[String]
      trans.read() returns p0
      val f0 = disp("zero")
      val f1 = disp("one")
      there was one(trans).write("zero")
      there was one(trans).read()
      f0.isDefined must beFalse
      f1.isDefined must beFalse

      "close transport and cancel pending requests" in {
        val intr = new Exception
        f0.raise(intr)
        there was one(trans).close()
        f0.poll must beLike {
          case Some(Throw(`intr`)) => true
        }
      }

      "ignore pending" in {
        val intr = new Exception
        f1.raise(intr)
        there was no(trans).close()
        f0.isDefined must beFalse
        f1.isDefined must beFalse

        p0.setValue("ok")
        f0.poll must beSome(Return("ok"))
        f1.poll must beLike {
          case Some(Throw(WriteException(`intr`))) => true
        }
        there was one(trans).write(any)
      }
    }

    "rewrite WriteExceptions" in {
      val exc = mock[Exception]
      trans.write(any) returns Future.exception(exc)
      val resultOpt = disp("hello").poll

      resultOpt.isDefined must beTrue
      resultOpt.get.isThrow must beTrue

      val result: Throwable = resultOpt.get.asInstanceOf[Throw[String]].e
      result.isInstanceOf[WriteException] must beTrue
      result.getCause must be_==(exc)
    }
  }
}
