package com.twitter.finagle.transport

import com.twitter.io.{Reader, Buf}
import com.twitter.util.{Await, Future, Promise, Time, Return, Throw}
import java.net.SocketAddress
import org.junit.runner.RunWith
import org.scalacheck.Prop.forAll
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.prop.GeneratorDrivenPropertyChecks

@RunWith(classOf[JUnitRunner])
class TransportTest extends FunSuite with GeneratorDrivenPropertyChecks {

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
    val isOpen = true
    val onClose = new Promise[Throwable]
    val localAddress = new SocketAddress {}
    val remoteAddress = new SocketAddress {}
    def close(deadline: Time) = Future.exception(new Exception)
  }

  class Failed extends Transport[Any, Any] {
    def write(in: Any) = Future.exception(new Exception)
    def read(): Future[Any] = Future.exception(new Exception)
    val onClose = new Promise[Throwable]
    val isOpen = false
    val localAddress = new SocketAddress {}
    val remoteAddress = new SocketAddress {}
    def close(deadline: Time) = Future.exception(new Exception)
  }

  test("Transport.copyToWriter - discard sets done") {
    val failed = new Failed { override def read() = Future.Done }
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
    assert(Await.result(f) === Some(Buf.Empty))
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
    assert(Await.result(f) === None)
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
      assert(Await.result(f) === Buf.Utf8(list.mkString))
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
      assert(result === exc)
      assert(done.isDefined)
    }
  }
}
