package com.twitter.finagle.dispatch

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import com.twitter.finagle.Service
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, Promise, Time}

class ServerDispatcherSpec extends SpecificationWithJUnit with Mockito {
  "SerialServerDispatcher" should {
    "dispatch one at a time" in {
      val trans = mock[Transport[String, String]]
      trans.onClose returns Future.never
      val service = mock[Service[String, String]]
      service.close(any) returns Future.Done

      val readp = new Promise[String]
      trans.read() returns readp

      val disp = new SerialServerDispatcher(trans, service)
      there was one(trans).read()
      there was no(trans).write(any)
      there was no(service)(any)

      val servicep = new Promise[String]
      service(any) returns servicep

      readp.setValue("ok")
      there was one(service)("ok")
      there was no(trans).write(any)

      val writep = new Promise[Unit]
      trans.write(any) returns writep

      servicep.setValue("ack")
      there was one(trans).write("ack")

      there was one(trans).read()
      trans.read() returns new Promise[String] // to short circuit
      writep.setValue(())
      there were two(trans).read()
    }

    "interrupt on hangup" in {
      val onClose = new Promise[Throwable]
      val trans = mock[Transport[String, String]]
      trans.onClose returns onClose
      val service = mock[Service[String, String]]
      service.close(any) returns Future.Done
      val replyp = new Promise[String] {
        @volatile var interrupted: Option[Throwable] = None
        setInterruptHandler { case exc => interrupted = Some(exc) }
      }
      service("ok") returns replyp

      val readp = new Promise[String]
      trans.read() returns readp

      val disp = new SerialServerDispatcher(trans, service)

      "while pending" in {
        readp.setValue("ok")
        there was one(service).apply("ok")
        replyp.interrupted must beNone
        onClose.setValue(new Exception)
        replyp.interrupted must beSomething
      }

      "while reading" in {
        onClose.setValue(new Exception)
        replyp.interrupted must beNone
        there was no(service).apply(any)
        readp.setValue("ok")
        there was one(service).apply("ok")
        replyp.interrupted must beSomething
        // This falls through.
        there was one(trans).close()
        there was one(service).close(any)
      }
    }

    "drain" in {
      val onClose = new Promise[Throwable]
      val trans = mock[Transport[String, String]]
      trans.onClose returns onClose
      val service = mock[Service[String, String]]
      service.close(any) returns Future.Done

      val readp = new Promise[String]
      trans.read() returns readp

      val disp = new SerialServerDispatcher(trans, service)
      there was one(trans).read()

      "while reading" in {
        disp.close(Time.now)
        there was one(trans).close(any)
        there was no(service).close(any)

        readp.setException(new Exception("closed!"))
        onClose.setValue(new Exception("closed!"))
        there was one(service).close(any)
        there was no(trans).write(any)
        there was one(trans).read()
      }

      "while dispatching" in {
        val servicep = new Promise[String]
        service(any) returns servicep
        readp.setValue("ok")
        there was one(service)("ok")
        disp.close(Time.now)
        there was no(service).close(any)
        there was no(trans).close()

        val writep = new Promise[Unit]
        trans.write(any) returns writep
        servicep.setValue("yes")
        there was one(trans).write("yes")

        there was no(service).close(any)
        there was no(trans).close()

        writep.setValue(())
        there was one(trans).close()
        onClose.setValue(new Exception("closed!"))
        there was one(service).close(any)

        there was one(trans).read()
      }
    }
  }
}
