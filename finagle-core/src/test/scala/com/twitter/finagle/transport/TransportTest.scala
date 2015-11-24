package com.twitter.finagle.transport

import com.twitter.finagle.Status
import com.twitter.io.{Reader, Buf}
import com.twitter.util.{Await, Future, Promise, Time, Return, Throw}
import com.twitter.concurrent.AsyncQueue
import java.net.SocketAddress
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class TransportTest extends FunSuite with GeneratorDrivenPropertyChecks {

  test("transport.map") {
    val q = new AsyncQueue[Any]
    val t0 = new QueueTransport[Any, Any](q, q)
    val trans = t0.map[String, String](_.toInt, _.toString)

    Await.result(trans.write("100"))
    assert(Await.result(trans.read()) == "100")

    Await.result(trans.write("10"))
    assert(Await.result(q.poll()) == 10)

    intercept[NumberFormatException] {
      Await.result(trans.write("hello"))
    }

    val exc = new Exception("can't coerce to string")
    q.offer(new Object {
      override def toString() = throw exc
    })

    assert(exc == intercept[Exception] {
      Await.result(trans.read())
    })
  }

  def fromList[A](seq: => List[A]) = new Transport[Any, Option[A]] {
    private[this] var next = seq
    def write(in: Any) = Future.exception(new Exception)
    def read() = synchronized {
      if (next.isEmpty) Future.None else {
        val head = next.head
        next = next.tail
        Future.value(Some(head))
      }
    }
    val status = Status.Open
    val onClose = new Promise[Throwable]
    val localAddress = new SocketAddress {}
    val remoteAddress = new SocketAddress {}
    val peerCertificate = None
    def close(deadline: Time) = Future.exception(new Exception)
  }

  class Failed extends Transport[Any, Any] {
    def write(in: Any) = Future.exception(new Exception)
    def read(): Future[Any] = Future.exception(new Exception)
    val onClose = new Promise[Throwable]
    val status = Status.Closed
    val localAddress = new SocketAddress {}
    val remoteAddress = new SocketAddress {}
    val peerCertificate = None
    def close(deadline: Time) = Future.exception(new Exception)
  }

  test("Transport.copyToWriter - discard while writing") {
    val failed = new Failed {
      override def read() = Future.Done
    }
    val reader = Reader.writable()
    val done = Transport.copyToWriter(failed, reader) { _ =>
      Future.value(Some(Buf.Empty))
    } respond {
      case Return(()) => reader.close()
      case Throw(exc) => reader.fail(exc)
      case _ =>
    }
    val f = reader.read(1)
    reader.discard()
    assert(Await.result(f) == Some(Buf.Empty))
    assert(done.isDefined)
    intercept[Reader.ReaderDiscarded] { Await.result(reader.read(1)) }
  }

  test("Transport.copyToWriter - concurrent reads") {
    val p = new Promise[Unit]
    val failed = new Failed { override def read() = p }
    val reader = Reader.writable()
    val done =
      Transport.copyToWriter(failed, reader)(_ => Future.None) respond {
        case Return(()) => reader.close()
        case Throw(exc) => reader.fail(exc)
        case _ =>
      }
    val f = reader.read(1)
    intercept[IllegalStateException] { Await.result(reader.read(1)) }
    p.setDone()
    assert(Await.result(f) == None)
  }

  test("Transport.copyToWriter - normal operation") {
    forAll { (list: List[String]) =>
      val t = fromList(list)
      val reader = Reader.writable()
      val done = Transport.copyToWriter(t, reader) {
        case None => Future.None
        case Some(str) => Future.value(Some(Buf.Utf8(str)))
      } respond {
        case Return(()) => reader.close()
        case Throw(exc) => reader.fail(exc)
        case _ =>
      }
      val f = Reader.readAll(reader)
      assert(Await.result(f) == Buf.Utf8(list.mkString))
      assert(done.isDefined)
    }
  }

  test("Transport.copyToWriter - failure") {
    forAll { (list: List[Byte]) =>
      val t = fromList(list)
      val exc = new Exception
      val reader = Reader.writable()
      val done = Transport.copyToWriter(t, reader) {
        case None => Future.exception(exc)
        case Some(b) => Future.value(Some(Buf.ByteArray(Array(b))))
      } respond {
        case Return(()) => reader.close()
        case Throw(exc) => reader.fail(exc)
        case _ =>
      }
      val f = Reader.readAll(reader)
      val result = intercept[Exception] { Await.result(f) }
      assert(result == exc)
      assert(done.isDefined)
    }
  }

  trait Collate {
    val writeq = new AsyncQueue[String]
    val readq = new AsyncQueue[String]
    val trans = new QueueTransport(writeq, readq)
    val fail = new Exception("fail")
    def read(string: String) = string match {
      case "eof" => Future.None
      case "fail" => Future.exception(fail)
      case x => Future.value(Some(Buf.Utf8(x)))
    }
    val coll = Transport.collate(trans, read)
    assert(!coll.isDefined)

    def assertDiscarded(f: Future[_]) {
      assert(f.isDefined)
      intercept[Reader.ReaderDiscarded] { Await.result(f) }
    }
  }

  test("Transport.collate: read through") (new Collate {
    // Long read
    val r1 = coll.read(10)
    assert(!r1.isDefined)
    readq.offer("hello")
    assert(Await.result(r1) == Some(Buf.Utf8("hello")))

    assert(!coll.isDefined)

    // Short read
    val r2 = coll.read(2)
    assert(!r2.isDefined)
    readq.offer("hello")
    assert(Await.result(r2) == Some(Buf.Utf8("he")))

    // Now, the EOF; but this isn't propagated yet.
    readq.offer("eof")
    assert(!coll.isDefined)

    val r3 = coll.read(10)
    assert(r3.isDefined)
    assert(Await.result(r3) == Some(Buf.Utf8("llo")))

    assert(coll.isDefined)
    Await.result(coll) // no exceptions

    // Further reads are EOF
    assert(Await.result(coll.read(10)) == None)
  })

  test("Transport.collate: discard while reading") (new Collate {
    val trans1 = new Transport[String, String] {
      val p = new Promise[String]
      var theIntr: Throwable = null
      p.setInterruptHandler {
        case intr =>
          theIntr = intr
      }
      def write(s: String) = ???
      def read() = p
      def status = ???
      val onClose = Future.never
      def localAddress = ???
      def remoteAddress = ???
      def peerCertificate = ???
      def close(deadline: Time) = ???
    }

    val coll1 = Transport.collate(trans1, read)
    val r1 = coll1.read(10)
    assert(!r1.isDefined)

    assert(trans1.theIntr == null)
    coll1.discard()
    assertDiscarded(r1)

    assert(!coll1.isDefined)
    assert(trans1.theIntr != null)
    assert(trans1.theIntr.isInstanceOf[Reader.ReaderDiscarded])

    // This is what a typical transport will do.
    trans1.p.setException(trans1.theIntr)
    assertDiscarded(coll1)
  })

  test("Transport.collate: discard while writing") (new Collate {
    readq.offer("hello")

    coll.discard()
    assertDiscarded(coll)
    assertDiscarded(coll.read(10))
  })

  test("Transport.collate: discard while buffering") (new Collate {
    readq.offer("hello")
    val r1 = coll.read(1)
    assert(Await.result(r1) == Some(Buf.Utf8("h")))

    coll.discard()
    assertDiscarded(coll)
    assertDiscarded(coll.read(10))
  })

  test("Transport.collate: conversion failure") (new Collate {
    readq.offer("hello")
    val r1 = coll.read(10)
    assert(Await.result(r1) == Some(Buf.Utf8("hello")))

    val r2 = coll.read(10)
    assert(!r2.isDefined)

    assert(!coll.isDefined)

    readq.offer("fail")

    assert(r2.isDefined)
    assert(r2.poll == Some(Throw(fail)))

    assert(coll.isDefined)
    assert(coll.poll == Some(Throw(fail)))
  })
}
