package com.twitter.finagle.transport

import com.twitter.conversions.DurationOps._
import com.twitter.concurrent.AsyncQueue
import com.twitter.finagle.Status
import com.twitter.io.{Buf, Pipe, BufReader, ReaderDiscardedException}
import com.twitter.util.{Await, Future, Promise, Return, Throw, Time}
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import scala.language.reflectiveCalls
import org.scalatest.funsuite.AnyFunSuite

class TransportTest extends AnyFunSuite with ScalaCheckDrivenPropertyChecks {

  private def awaitResult[T](f: Future[T]): T = {
    Await.result(f, 5.seconds)
  }

  test("transport.cast of invalid type") {
    val q = new AsyncQueue[Any]
    val writeQueue = new AsyncQueue[Any]
    val t0 = new QueueTransport[Any, Any](writeQueue, q)
    val trans: Transport[Int, String] =
      Transport.cast[Int, String](t0)

    val s = "a string"
    q.offer(s)

    val sOut: String = awaitResult(trans.read())
    assert(s == sOut)

    q.offer(0)

    intercept[ClassCastException] {
      // Fails because the Transport was cast from Any to String, which is enforced
      // at runtime, even if we lower the static type to Any.
      val s2Out: Any = awaitResult(trans.read())
    }

    // write Ints out of the transport
    awaitResult(trans.write(1)) // should finish immediately
    awaitResult(writeQueue.poll()) match {
      case i: Int => assert(i == 1)
      case other => fail(s"Expected Int(1), received: $other")
    }
  }

  test("transport.cast of sub types") {
    trait Foo
    case class Bar(i: Int) extends Foo

    val q = new AsyncQueue[Any]
    val unused = new AsyncQueue[Any]
    val t0 = new QueueTransport[Any, Any](unused, q)
    val trans: Transport[Any, Foo] =
      Transport.cast[Any, Foo](t0)

    q.offer(Bar(1))

    val foo = awaitResult(trans.read())
    assert(foo == Bar(1))
  }

  test("transport.map") {
    val q = new AsyncQueue[Any]
    val t0 = new QueueTransport[Any, Any](q, q)
    val trans = t0.map[String, String](_.toInt, _.toString)

    awaitResult(trans.write("100"))
    assert(awaitResult(trans.read()) == "100")

    awaitResult(trans.write("10"))
    assert(awaitResult(q.poll()) == 10)

    // We perform the write outside of the intercept since errors must be captured in the Future
    val writeFuture = trans.write("hello")
    intercept[NumberFormatException] {
      awaitResult(writeFuture)
    }

    val exc = new Exception("can't coerce to string")
    q.offer(new Object {
      override def toString() = throw exc
    })

    // We perform the read outside of the intercept since errors must be captured in the Future
    val readFuture = trans.read()
    assert(exc == intercept[Exception] {
      awaitResult(readFuture)
    })
  }

  def fromList[A](seq: => List[A]) = new Transport[Any, Option[A]] {
    type Context = TransportContext
    private[this] var next = seq
    def write(in: Any) = Future.exception(new Exception)
    def read() = synchronized {
      if (next.isEmpty) Future.None
      else {
        val head = next.head
        next = next.tail
        Future.value(Some(head))
      }
    }
    val status = Status.Open
    val onClose = new Promise[Throwable]
    def close(deadline: Time) = Future.exception(new Exception)
    val context: TransportContext = new SimpleTransportContext()
  }

  class Failed extends Transport[Any, Any] {
    type Context = TransportContext
    def write(in: Any) = Future.exception(new Exception)
    def read(): Future[Any] = Future.exception(new Exception)
    val onClose = new Promise[Throwable]
    val status = Status.Closed
    def close(deadline: Time) = Future.exception(new Exception)
    val context: TransportContext = new SimpleTransportContext()
  }

  test("Transport.copyToWriter - discard while writing") {
    val failed = new Failed {
      override def read() = Future.Done
    }
    val reader = new Pipe[Buf]()
    val done = Transport.copyToWriter(failed, reader) { _ =>
      Future.value(Some(Buf.Empty))
    } respond {
      case Return(()) => reader.close()
      case Throw(exc) => reader.fail(exc)
      case _ =>
    }
    val f = reader.read()
    reader.discard()
    assert(awaitResult(f) == Some(Buf.Empty))
    assert(done.isDefined)
    intercept[ReaderDiscardedException] { awaitResult(reader.read()) }
  }

  test("Transport.copyToWriter - concurrent reads") {
    val p = new Promise[Unit]
    val failed = new Failed { override def read() = p }
    val reader = new Pipe[Buf]()
    val done =
      Transport.copyToWriter(failed, reader)(_ => Future.None) respond {
        case Return(()) => reader.close()
        case Throw(exc) => reader.fail(exc)
        case _ =>
      }
    val f = reader.read()
    intercept[IllegalStateException] { awaitResult(reader.read()) }
    p.setDone()
    assert(awaitResult(f) == None)
  }

  test("Transport.copyToWriter - normal operation") {
    forAll { (list: List[String]) =>
      val t = fromList(list)
      val reader = new Pipe[Buf]()
      val done = Transport.copyToWriter(t, reader) {
        case None => Future.None
        case Some(str) => Future.value(Some(Buf.Utf8(str)))
      } respond {
        case Return(()) => reader.close()
        case Throw(exc) => reader.fail(exc)
        case _ =>
      }
      val f = BufReader.readAll(reader)
      assert(awaitResult(f) == Buf.Utf8(list.mkString))
      assert(done.isDefined)
    }
  }

  test("Transport.copyToWriter - failure") {
    forAll { (list: List[Byte]) =>
      val t = fromList(list)
      val exc = new Exception
      val reader = new Pipe[Buf]()
      val done = Transport.copyToWriter(t, reader) {
        case None => Future.exception(exc)
        case Some(b) => Future.value(Some(Buf.ByteArray.Owned(Array(b))))
      } respond {
        case Return(()) => reader.close()
        case Throw(exc) => reader.fail(exc)
        case _ =>
      }
      val f = BufReader.readAll(reader)
      val result = intercept[Exception] { awaitResult(f) }
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

    def assertDiscarded(f: Future[_]): Unit = {
      assert(f.isDefined)
      intercept[ReaderDiscardedException] { awaitResult(f) }
    }
  }

  test("Transport.collate: read through")(new Collate {
    // Long read
    val r1 = coll.read()
    assert(!r1.isDefined)

    readq.offer("hello")
    readq.offer("eof")

    assert(awaitResult(r1).contains(Buf.Utf8("hello")))
    assert(awaitResult(coll.read()).isEmpty) // Further reads are EOF
    assert(coll.isDefined)
    awaitResult(coll) // no exceptions
  })

  test("Transport.collate: discard while reading")(new Collate {
    val trans1 = new Transport[String, String] {
      type Context = TransportContext
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
      def close(deadline: Time) = ???
      val context: TransportContext = new SimpleTransportContext()
    }

    val coll1 = Transport.collate(trans1, read)
    val r1 = coll1.read()
    assert(!r1.isDefined)

    assert(trans1.theIntr == null)
    coll1.discard()
    assertDiscarded(r1)

    assert(!coll1.isDefined)
    assert(trans1.theIntr != null)
    assert(trans1.theIntr.isInstanceOf[ReaderDiscardedException])

    // This is what a typical transport will do.
    trans1.p.setException(trans1.theIntr)
    assertDiscarded(coll1)
  })

  test("Transport.collate: discard while writing")(new Collate {
    readq.offer("hello")

    coll.discard()
    assertDiscarded(coll)
    assertDiscarded(coll.read())
  })

  test("Transport.collate: discard while buffering")(new Collate {
    readq.offer("hello")
    readq.offer("world")

    val r1 = coll.read()
    assert(awaitResult(r1).contains(Buf.Utf8("hello")))

    coll.discard()
    assertDiscarded(coll)
    assertDiscarded(coll.read())
  })

  test("Transport.collate: conversion failure")(new Collate {
    readq.offer("hello")
    val r1 = coll.read()
    assert(awaitResult(r1) == Some(Buf.Utf8("hello")))

    val r2 = coll.read()
    assert(!r2.isDefined)

    assert(!coll.isDefined)

    readq.offer("fail")

    assert(r2.isDefined)
    assert(r2.poll == Some(Throw(fail)))

    assert(coll.isDefined)
    assert(coll.poll == Some(Throw(fail)))
  })
}
