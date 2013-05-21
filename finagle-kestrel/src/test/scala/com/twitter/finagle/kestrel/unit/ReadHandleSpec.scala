package com.twitter.finagle.kestrel.unit

import com.twitter.concurrent.Broker
import com.twitter.finagle.kestrel._
import com.twitter.util.Await
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.util.CharsetUtil
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class ReadHandleSpec extends SpecificationWithJUnit with Mockito {
  def msg_(i: Int) = {
    val ack = new Broker[Unit]
    (ack.recv, ReadMessage(ChannelBuffers.wrappedBuffer(i.toString.getBytes), ack.send(())))
  }

  def msg(i: Int) = { val (_, m) = msg_(i); m }

  "ReadHandle.buffered" should {
    val N = 10
    val messages = new Broker[ReadMessage]
    val error = new Broker[Throwable]
    val close = new Broker[Unit]
    val handle = ReadHandle(messages.recv, error.recv, close.send(()))
    val buffered = handle.buffered(N)

    "acknowledge howmany messages" in {
      0 until N foreach { i =>
        val (ack, m) = msg_(i)
        messages ! m
        (ack?).isDefined must beTrue
      }
      val (ack, m) = msg_(0)
      (ack?).isDefined must beFalse
    }

    "not synchronize on send when buffer is full" in {
      0 until N foreach { _ =>
        (messages ! msg(0)).isDefined must beTrue
      }
      (messages ! msg(0)).isDefined must beFalse
    }

    "keep the buffer full" in {
      0 until N foreach { _ =>
        messages ! msg(0)
      }
      val sent = messages ! msg(0)
      sent.isDefined must beFalse
      val recvd = (buffered.messages?)
      recvd.isDefined must beTrue
      Await.result(recvd).ack.sync()
      sent.isDefined must beTrue
    }

    "preserve FIFO order" in {
      0 until N foreach { i =>
        messages ! msg(i)
      }

      0 until N foreach { i =>
        val recvd = (buffered.messages?)
        recvd.isDefined must beTrue
        Await.result(recvd).bytes.toString(CharsetUtil.UTF_8) must be_==(i.toString)
      }
    }

    "propagate errors" in {
      val errd = (buffered.error?)
      errd.isDefined must beFalse
      val e = new Exception("sad panda")
      error ! e
      errd.isDefined must beTrue
      Await.result(errd) must be(e)
    }

    "when closed" in {
      "propagate immediately if empty" in {
        val closed = (close?)
        closed.isDefined must beFalse
        buffered.close()
        closed.isDefined must beTrue
      }

      "wait for outstanding acks before closing underlying" in {
        val closed = (close?)
        closed.isDefined must beFalse
        messages ! msg(0)
        messages ! msg(1)
        buffered.close()
        closed.isDefined must beFalse
        val m0 = (buffered.messages?)
        m0.isDefined must beTrue
        Await.result(m0).ack.sync()
        closed.isDefined must beFalse
        val m1 = (buffered.messages?)
        m1.isDefined must beTrue
        Await.result(m1).ack.sync()
        closed.isDefined must beTrue
      }
    }
  }

  "ReadHandle.merged" should {
    val messages0 = new Broker[ReadMessage]
    val error0 = new Broker[Throwable]
    val close0 = new Broker[Unit]
    val handle0 = ReadHandle(messages0.recv, error0.recv, close0.send(()))
    val messages1 = new Broker[ReadMessage]
    val error1 = new Broker[Throwable]
    val close1 = new Broker[Unit]
    val handle1 = ReadHandle(messages1.recv, error1.recv, close1.send(()))

    val merged = ReadHandle.merged(Seq(handle0, handle1))

    "provide a merged stream of messages" in {
      var count = 0
      merged.messages.foreach { _ => count += 1 }
      count must be_==(0)

      messages0 ! msg(0)
      messages1 ! msg(1)
      messages0 ! msg(2)

      count must be_==(3)
    }

   "provide a merged stream of errors" in {
      var count = 0
      merged.error.foreach { _ => count += 1 }
      count must be_==(0)

      error0 ! new Exception("sad panda")
      error1 ! new Exception("sad panda #2")

      count must be_==(2)
    }

    "propagate closes to all underlying handles" in {
      val closed0 = (close0?)
      val closed1 = (close1?)

      closed0.isDefined must beFalse
      closed1.isDefined must beFalse

      merged.close()

      closed0.isDefined must beTrue
      closed1.isDefined must beTrue
    }
  }
}
