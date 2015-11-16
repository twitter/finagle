package com.twitter.finagle.kestrel.unit

import com.twitter.concurrent.Broker
import com.twitter.finagle.kestrel.{ReadHandle, ReadMessage}
import com.twitter.io.{Buf, Charsets}
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ReadHandleTest extends FunSuite {
  def msg_(i: Int) = {
    val ack = new Broker[Unit]
    val abort = new Broker[Unit]
    (ack.recv, ReadMessage(Buf.Utf8(i.toString), ack.send(()), abort.send(())))
  }

  def msg(i: Int) = { val (_, m) = msg_(i); m }

  trait BufferedReadHandle {
    val N = 10
    val messages = new Broker[ReadMessage]
    val error = new Broker[Throwable]
    val close = new Broker[Unit]
    val handle = ReadHandle(messages.recv, error.recv, close.send(()))
    val buffered = handle.buffered(N)
  }

  trait MergedReadHandle {
    val messages0 = new Broker[ReadMessage]
    val error0 = new Broker[Throwable]
    val close0 = new Broker[Unit]
    val handle0 = ReadHandle(messages0.recv, error0.recv, close0.send(()))
    val messages1 = new Broker[ReadMessage]
    val error1 = new Broker[Throwable]
    val close1 = new Broker[Unit]
    val handle1 = ReadHandle(messages1.recv, error1.recv, close1.send(()))

    val merged = ReadHandle.merged(Seq(handle0, handle1))
  }

  test("ReadHandle.buffered should acknowledge howmany messages") {
    new BufferedReadHandle {
      0 until N foreach { i =>
        val (ack, m) = msg_(i)
        messages ! m
        assert((ack ?).isDefined == true)
      }
      val (ack, m) = msg_(0)
      assert((ack ?).isDefined == false)
    }
  }

  test("ReadHandle.buffered should not synchronize on send when buffer is full") {
    new BufferedReadHandle {
      0 until N foreach { _ =>
        assert((messages ! msg(0)).isDefined == true)
      }
      assert((messages ! msg(0)).isDefined == false)
    }
  }

  test("ReadHandle.buffered should keep the buffer full") {
    new BufferedReadHandle {
      0 until N foreach { _ =>
        messages ! msg(0)
      }
      val sent = messages ! msg(0)
      assert(sent.isDefined == false)
      val recvd = (buffered.messages ?)
      assert(recvd.isDefined == true)
      Await.result(recvd).ack.sync()
      assert(sent.isDefined == true)
    }
  }

  test("ReadHandle.buffered should preserve FIFO order") {
    new BufferedReadHandle {
      0 until N foreach { i =>
        messages ! msg(i)
      }

      0 until N foreach { i =>
        val recvd = (buffered.messages ?)
        assert(recvd.isDefined == true)
        val Buf.Utf8(res) = Await.result(recvd).bytes

        assert(res == i.toString)
      }
    }
  }

  test("ReadHandle.buffered should propagate errors") {
    new BufferedReadHandle {
      val errd = (buffered.error ?)
      assert(errd.isDefined == false)
      val e = new Exception("sad panda")
      error ! e
      assert(errd.isDefined == true)
      assert(Await.result(errd) == e)
    }
  }

  test("ReadHandle.buffered should when closed propagate immediately if empty") {
    new BufferedReadHandle {
      val closed = (close ?)
      assert(closed.isDefined == false)
      buffered.close()
      assert(closed.isDefined == true)
    }
  }

  test("ReadHandle.buffered should when closed wait for outstanding acks before closing underlying") {
    new BufferedReadHandle {
      val closed = (close ?)
      assert(closed.isDefined == false)
      messages ! msg(0)
      messages ! msg(1)
      buffered.close()
      assert(closed.isDefined == false)
      val m0 = (buffered.messages ?)
      assert(m0.isDefined == true)
      Await.result(m0).ack.sync()
      assert(closed.isDefined == false)
      val m1 = (buffered.messages ?)
      assert(m1.isDefined == true)
      Await.result(m1).ack.sync()
      assert(closed.isDefined == true)
    }
  }

  test("ReadHandle.merged should") {
    new MergedReadHandle {
      var count = 0
      merged.messages.foreach { _ => count += 1 }
      assert(count == 0)

      messages0 ! msg(0)
      messages1 ! msg(1)
      messages0 ! msg(2)

      assert(count == 3)
    }
  }

  test("ReadHandle.merged should provide a merged stream of errors provide a merged stream of messages") {
    new MergedReadHandle {
      var count = 0
      merged.error.foreach { _ => count += 1 }
      assert(count == 0)

      error0 ! new Exception("sad panda")
      error1 ! new Exception("sad panda #2")

      assert(count == 2)
    }
  }

  test("ReadHandle.merged should propagate closes to all underlying handles") {
    new MergedReadHandle {
      val closed0 = close0 ?
      val closed1 = close1 ?

      assert(closed0.isDefined == false)
      assert(closed1.isDefined == false)

      merged.close()

      assert(closed0.isDefined == true)
      assert(closed1.isDefined == true)
    }
  }
}
