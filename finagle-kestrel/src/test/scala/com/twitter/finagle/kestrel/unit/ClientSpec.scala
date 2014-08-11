package com.twitter.finagle.kestrel
package unit

import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Suites}
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.{verify, times, when}

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import com.twitter.util.{Await, Future, Duration, Time, MockTimer, Promise}
import com.twitter.concurrent.{Offer, Broker}
import com.twitter.conversions.time._

import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.finagle.kestrel._
import com.twitter.finagle.kestrel.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import com.twitter.finagle.thrift.ThriftClientRequest
import com.twitter.finagle.kestrel.net.lag.kestrel.thriftscala.Item

@RunWith(classOf[JUnitRunner])
class ClientTest extends Suites(
  new ClientReadReliablyTest, 
  new ConnectedClientReadTest, 
  new ThriftConnectedClientReadTest
)

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

class ClientReadReliablyTest extends FunSuite with MockitoSugar {
  def buf(i: Int) = ChannelBuffers.wrappedBuffer("%d".format(i).getBytes)
  def msg(i: Int) = {
    val m = mock[ReadMessage]
    when(m.bytes).thenReturn(buf(i))
    m
  }

  val messages = new Broker[ReadMessage]
  val error = new Broker[Throwable]
  val client = mock[Client]
  val rh = mock[ReadHandle]
  when(rh.messages).thenReturn(messages.recv)
  when(rh.error).thenReturn(error.recv)
  when(client.read("foo")).thenReturn(rh)

  test("proxy messages") {
    val h = client.readReliably("foo")
    verify(client).read("foo")

    val f = (h.messages?)
    assert(f.isDefined === false)

    val m = msg(0)

    messages ! m
    assert(f.isDefined === true)
    assert(Await.result(f) === m)

    assert((h.messages?).isDefined === false)
  }

  test("reconnect on failure") {
    val h = client.readReliably("foo")
    verify(client).read("foo")
    val m = msg(0)
    messages ! m
    assert((h.messages??) === m)

    val messages2 = new Broker[ReadMessage]
    val error2 = new Broker[Throwable]
    val rh2 = mock[ReadHandle]
    when(rh2.messages).thenReturn(messages2.recv)
    when(rh2.error).thenReturn(error2.recv)
    when(client.read("foo")).thenReturn(rh2)

    error ! new Exception("wtf")
    verify(client, times(2)).read("foo")

    messages ! m  // an errant message on broken channel

    // new messages must make it
    val f = (h.messages?)
    assert(f.isDefined === false)

    val m2 = msg(2)
    messages2 ! m2
    assert(f.isDefined === true)
    assert(Await.result(f) === m2)
  }

  // test("reconnect on failure (with delay)") in Time.withCurrentTimeFrozen { tc =>
  //   val timer = new MockTimer
  //   val delays = Stream(1.seconds, 2.seconds, 3.second)
  //   val h = client.readReliably("foo", timer, delays)
  //   verify(client).read("foo")

  //   val errf = (h.error?)

  //   delays.zipWithIndex foreach { case (delay, i) =>
  //     verify(client, times(i + 1)).read("foo")
  //     error ! new Exception("sad panda")
  //     tc.advance(delay)
  //     timer.tick()
  //     verify(client, times(i + 2)).read("foo")
  //     assert(errf.isDefined === false)
  //   }

  //   error ! new Exception("final sad panda")

  //   assert(errf.isDefined === true)
  //   intercept[OutOfRetriesException] {
  //     Await.result(errf)
  //   }
  // }

  test("close on close requested") {
    val h = client.readReliably("foo")
    verify(rh, times(0)).close()
    h.close()
    verify(rh).close()
  }
}

class ConnectedClientReadTest extends FunSuite with MockitoSugar {
  val queueName = "foo"
  val factory = mock[ServiceFactory[Command, Response]]
  val service = mock[Service[Command, Response]]
  val client = new ConnectedClient(factory)
  val open = Open(queueName, Some(Duration.Top))
  val closeAndOpen = CloseAndOpen(queueName, Some(Duration.Top))
  val abort = Abort(queueName)

  test("interrupt current request on close") {
    when(factory.apply()).thenReturn(Future(service))
    val promise = new Promise[Response]()
    @volatile var wasInterrupted = false
    promise.setInterruptHandler { case _cause =>
      wasInterrupted = true
    }
    when(service(open)).thenReturn(promise)
    when(service(closeAndOpen)).thenReturn(promise)
    when(service(abort)).thenReturn(Future(Values(Seq())))

    val rh = client.read(queueName)

    assert(wasInterrupted === false)
    rh.close()
    assert(wasInterrupted === true)
  }
}

class ThriftConnectedClientReadTest extends FunSuite with MockitoSugar {
  val queueName = "foo"
  val clientFactory = mock[FinagledClientFactory]
  val finagledClient = mock[FinagledClosableClient]
  val client = new ThriftConnectedClient(clientFactory)

  test("interrupt current thrift request on close") {
    when(clientFactory.apply()).thenReturn(Future(finagledClient))
    val promise = new Promise[Seq[Item]]()

    @volatile var wasInterrupted = false
    promise.setInterruptHandler { case _cause =>
      wasInterrupted = true
    }

    when(finagledClient.get(queueName, 1, Int.MaxValue, Int.MaxValue)).thenReturn(promise)

    val rh = client.read(queueName)

    assert(wasInterrupted === false)
    rh.close()
    assert(wasInterrupted === true)
  }
}