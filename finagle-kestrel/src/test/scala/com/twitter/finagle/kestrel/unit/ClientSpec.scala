package com.twitter.finagle.kestrel
package unit

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import com.twitter.util.{Await, Future, Duration, Time, MockTimer, Promise}
import com.twitter.concurrent.{Offer, Broker}
import com.twitter.conversions.time._

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.finagle.kestrel._
import com.twitter.finagle.kestrel.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._

// all this so we can spy() on a client.
class MockClient extends Client {
  def set(queueName: String, value: ChannelBuffer, expiry: Time = Time.epoch) = null
  def get(queueName: String, waitUpTo: Duration = 0.seconds): Future[Option[ChannelBuffer]] = null
  def delete(queueName: String): Future[Response] = null
  def flush(queueName: String): Future[Response] = null
  def read(queueName: String): ReadHandle = null
  def write(queueName: String, offer: Offer[ChannelBuffer]): Future[Throwable] = null
  def close() {}
}

class ClientSpec extends SpecificationWithJUnit with Mockito {
  def buf(i: Int) = ChannelBuffers.wrappedBuffer("%d".format(i).getBytes)
  def msg(i: Int) = {
    val m = mock[ReadMessage]
    m.bytes returns buf(i)
    m
  }

  "Client.readReliably" should {
    val messages = new Broker[ReadMessage]
    val error = new Broker[Throwable]
    val client = spy(new MockClient)
    val rh = mock[ReadHandle]
    rh.messages returns messages.recv
    rh.error returns error.recv
    client.read("foo") returns rh

    "proxy messages" in {
      val h = client.readReliably("foo")
      there was one(client).read("foo")

      val f = (h.messages?)
      f.isDefined must beFalse

      val m = msg(0)

      messages ! m
      f.isDefined must beTrue
      Await.result(f) must be(m)

      (h.messages?).isDefined must beFalse
    }

    "reconnect on failure" in {
      val h = client.readReliably("foo")
      there was one(client).read("foo")
      val m = msg(0)
      messages ! m
      (h.messages??) must be(m)

      val messages2 = new Broker[ReadMessage]
      val error2 = new Broker[Throwable]
      val rh2 = mock[ReadHandle]
      rh2.messages returns messages2.recv
      rh2.error returns error2.recv
      client.read("foo") returns rh2

      error ! new Exception("wtf")
      there were two(client).read("foo")

      messages ! m  // an errant message on broken channel

      // new messages must make it
      val f = (h.messages?)
      f.isDefined must beFalse

      val m2 = msg(2)
      messages2 ! m2
      f.isDefined must beTrue
      Await.result(f) must be(m2)
    }

    "reconnect on failure (with delay)" in Time.withCurrentTimeFrozen { tc =>
      val timer = new MockTimer
      val delays = Stream(1.seconds, 2.seconds, 3.second)
      val h = client.readReliably("foo", timer, delays)
      there was one(client).read("foo")

      val errf = (h.error?)

      delays.zipWithIndex foreach { case (delay, i) =>
        there were (i + 1).times(client).read("foo")
        error ! new Exception("sad panda")
        tc.advance(delay)
        timer.tick()
        there were (i + 2).times(client).read("foo")
        errf.isDefined must beFalse
      }

      error ! new Exception("final sad panda")

      errf.isDefined must beTrue
      Await.result(errf) must be_==(OutOfRetriesException)
    }

    "close on close requested" in {
      val h = client.readReliably("foo")
      there was no(rh).close()
      h.close()
      there was one(rh).close()
    }
  }

  "ConnectedClient.read" should {
    val queueName = "foo"
    val factory = mock[ServiceFactory[Command, Response]]
    val service = mock[Service[Command, Response]]
    val client = new ConnectedClient(factory)
    val open = Open(queueName, Some(Duration.Top))
    val closeAndOpen = CloseAndOpen(queueName, Some(Duration.Top))
    val abort = Abort(queueName)

    "interrupt current request on close" in {
      factory.apply() returns Future(service)
      val promise = new Promise[Response]()
      @volatile var wasInterrupted = false
      promise.setInterruptHandler { case _cause =>
        wasInterrupted = true
      }
      service(open) returns promise
      service(closeAndOpen) returns promise
      service(abort) returns Future(Values(Seq()))

      val rh = client.read(queueName)

      wasInterrupted must beFalse
      rh.close()
      wasInterrupted must beTrue
    }
  }
}
