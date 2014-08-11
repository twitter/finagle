package com.twitter.finagle.kestrel.unit

import com.twitter.concurrent.Broker
import com.twitter.finagle.kestrel._
import com.twitter.io.Charsets
import com.twitter.util.Await
import org.jboss.netty.buffer.ChannelBuffers

import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Suites}
import org.scalatest.junit.JUnitRunner

trait Messaging {
  def msg_(i: Int) = {
    val ack = new Broker[Unit]
    val abort = new Broker[Unit]
    (ack.recv, ReadMessage(ChannelBuffers.wrappedBuffer(i.toString.getBytes), ack.send(()), abort.send(())))
  }

  def msg(i: Int) = { val (_, m) = msg_(i); m }
}

@RunWith(classOf[JUnitRunner])
class ReadHandleTest extends Suites(
   new ReadHandleBufferedTest, 
   new ReadHandleMergedTest
)

class ReadHandleBufferedTest extends FunSuite with Messaging {
  val N = 10
  val messages = new Broker[ReadMessage]
  val error = new Broker[Throwable]
  val close = new Broker[Unit]
  val handle = ReadHandle(messages.recv, error.recv, close.send(()))
  val buffered = handle.buffered(N)

  test("acknowledge howmany messages") {
    0 until N foreach { i =>
      val (ack, m) = msg_(i)
      messages ! m
      assert((ack?).isDefined)
    }
    val (ack, m) = msg_(0)
    assert(!(ack?).isDefined)
  }

  test("not synchronize on send when buffer is full") {
    0 until N foreach { _ =>
      assert((messages ! msg(0)).isDefined)
    }
    assert(!(messages ! msg(0)).isDefined)
  }

  test("keep the buffer full") {
    0 until N foreach { _ =>
      messages ! msg(0)
    }
    val sent = messages ! msg(0)
    assert(!sent.isDefined)
    val recvd = (buffered.messages?)
    assert(recvd.isDefined)
    Await.result(recvd).ack.sync()
    assert(sent.isDefined)
  }

  test("preserve FIFO order") {
    0 until N foreach { i =>
      messages ! msg(i)
    }

    0 until N foreach { i =>
      val recvd = (buffered.messages?)
      assert(recvd.isDefined)
      assert(Await.result(recvd).bytes.toString(Charsets.Utf8) === i.toString)
    }
  }

  test("propagate errors") {
    val errd = (buffered.error?)
    assert(!errd.isDefined)
    val e = new Exception("Sad panda")
    error ! e
    assert(errd.isDefined)
    intercept[Exception] {
      Await.result(errd)
    }
  }

  test("when closed, propagate immediately if empty") {
    val closed = (close?)
    assert(!closed.isDefined)
    buffered.close()
    assert(closed.isDefined)
  }

  test("when closed, wait for outstanding acks before closing underlying") {
    val closed = (close?)
    assert(!closed.isDefined)
    messages ! msg(0)
    messages ! msg(1)
    buffered.close()
    assert(!closed.isDefined)
    val m0 = (buffered.messages?)
    assert(m0.isDefined)
    Await.result(m0).ack.sync()
    assert(!closed.isDefined)
    val m1 = (buffered.messages?)
    assert(m1.isDefined)
    Await.result(m1).ack.sync()
    assert(closed.isDefined)
  }
}

class ReadHandleMergedTest extends FunSuite with Messaging {
  val messages0 = new Broker[ReadMessage]
  val error0 = new Broker[Throwable]
  val close0 = new Broker[Unit]
  val handle0 = ReadHandle(messages0.recv, error0.recv, close0.send(()))
  val messages1 = new Broker[ReadMessage]
  val error1 = new Broker[Throwable]
  val close1 = new Broker[Unit]
  val handle1 = ReadHandle(messages1.recv, error1.recv, close1.send(()))

  val merged = ReadHandle.merged(Seq(handle0, handle1))

  test("provide a merged stream of messages") {
    var count = 0
    merged.messages.foreach { _ => count += 1 }
    assert(count === 0)

    messages0 ! msg(0)
    messages1 ! msg(1)
    messages0 ! msg(2)

    assert(count === 3)
  }

 test("provide a merged stream of errors") {
    var count = 0
    merged.error.foreach { _ => count += 1 }
    assert(count === 0)

    error0 ! new Exception("sad panda")
    error1 ! new Exception("sad panda #2")

    assert(count === 2)
  }

  test("propagate closes to all underlying handles") {
    val closed0 = (close0?)
    val closed1 = (close1?)

    assert(!closed0.isDefined)
    assert(!closed1.isDefined)

    merged.close()

    assert(closed0.isDefined)
    assert(closed1.isDefined)
  }
}