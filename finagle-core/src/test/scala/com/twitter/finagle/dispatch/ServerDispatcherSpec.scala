package com.twitter.finagle.dispatch

import org.specs.Specification
import org.specs.mock.Mockito
import com.twitter.finagle.Service
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Future, Promise}

object ServerDispatcherSpec extends Specification with Mockito {
  "SerialServerDispatcher" should {
    "dispatch one at a time" in {
      val trans = mock[Transport[String, String]]
      val service = mock[Service[String, String]]

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

    "drain" in {
      val trans = mock[Transport[String, String]]
      val service = mock[Service[String, String]]

      val readp = new Promise[String]
      trans.read() returns readp

      val disp = new SerialServerDispatcher(trans, service)
      there was one(trans).read()

      "while reading" in {
        disp.drain()
        there was one(trans).close()
        there was no(service).release()

        readp.setException(new Exception("closed!"))
        there was one(service).release()
        there was no(trans).write(any)
        there was one(trans).read()
      }

      "while dispatching" in {
        val servicep = new Promise[String]
        service(any) returns servicep
        readp.setValue("ok")
        there was one(service)("ok")
        disp.drain()
        there was no(service).release()
        there was no(trans).close()

        val writep = new Promise[Unit]
        trans.write(any) returns writep
        servicep.setValue("yes")
        there was one(trans).write("yes")

        there was no(service).release()
        there was no(trans).close()

        writep.setValue(())
        there was one(service).release()
        there was one(trans).close()

        there was one(trans).read()
      }
    }
  }
}
