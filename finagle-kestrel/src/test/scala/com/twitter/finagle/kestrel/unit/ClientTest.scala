package com.twitter.finagle.kestrel.unit

import com.twitter.concurrent.{Broker, Offer}
import com.twitter.conversions.time._
import com.twitter.finagle.kestrel._
import com.twitter.finagle.kestrel.net.lag.kestrel.thriftscala.Item
import com.twitter.finagle.kestrel.protocol.{Command, _}
import com.twitter.finagle.{Service, ServiceFactory}
import com.twitter.io.Buf
import com.twitter.util._
import org.junit.runner.RunWith
import org.mockito.Mockito
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

// all this so we can spy() on a client.
class MockClient extends Client {
  def set(queueName: String, value: Buf, expiry: Time = Time.epoch) = null
  def get(queueName: String, waitUpTo: Duration = 0.seconds): Future[Option[Buf]] = null
  def delete(queueName: String): Future[Response] = null
  def flush(queueName: String): Future[Response] = null
  def read(queueName: String): ReadHandle = null
  def write(queueName: String, offer: Offer[Buf]): Future[Throwable] = null
  def close() {}
}

@RunWith(classOf[JUnitRunner])
class ClientTest extends FunSuite with MockitoSugar {
  trait GlobalHelper {
    def buf(i: Int) = Buf.Utf8(i.toString)
    def msg(i: Int) = {
      val m = mock[ReadMessage]
      when(m.bytes) thenReturn buf(i)
      m
    }
  }

  trait ClientReliablyHelper extends GlobalHelper {
    val messages = new Broker[ReadMessage]
    val error = new Broker[Throwable]
    val client = Mockito.spy(new MockClient)
    val rh = mock[ReadHandle]
    when(rh.messages) thenReturn messages.recv
    when(rh.error) thenReturn error.recv
    when(client.read("foo")) thenReturn rh
  }

  test("Client.readReliably should proxy messages") {
    new ClientReliablyHelper {
      val h = client.readReliably("foo")
      verify(client).read("foo")

      val f = (h.messages ?)
      assert(f.isDefined == false)

      val m = msg(0)

      messages ! m
      assert(f.isDefined == true)
      assert(Await.result(f) == m)

      assert((h.messages ?).isDefined == false)
    }
  }

  test("Client.readReliably should reconnect on failure") {
    new ClientReliablyHelper {
      val h = client.readReliably("foo")
      verify(client).read("foo")
      val m = msg(0)
      messages ! m
      assert((h.messages ??) == m)

      val messages2 = new Broker[ReadMessage]
      val error2 = new Broker[Throwable]
      val rh2 = mock[ReadHandle]
      when(rh2.messages) thenReturn messages2.recv
      when(rh2.error) thenReturn error2.recv
      when(client.read("foo")) thenReturn rh2

      error ! new Exception("wtf")
      verify(client, times(2)).read("foo")

      messages ! m
      // an errant message on broken channel

      // new messages must make it
      val f = (h.messages ?)
      assert(f.isDefined == false)

      val m2 = msg(2)
      messages2 ! m2
      assert(f.isDefined == true)
      assert(Await.result(f) == m2)
    }
  }

  test("Client.readReliably should reconnect on failure(with delay)") {
    Time.withCurrentTimeFrozen { tc =>
      new ClientReliablyHelper {
        val timer = new MockTimer
        val delays = Stream(1.seconds, 2.seconds, 3.second)
        val h = client.readReliably("foo", timer, delays)
        verify(client).read("foo")

        val errf = (h.error ?)

        delays.zipWithIndex foreach { case (delay, i) =>
          verify(client, times(i + 1)).read("foo")
          error ! new Exception("sad panda")
          tc.advance(delay)
          timer.tick()
          verify(client, times(i + 2)).read("foo")
          assert(errf.isDefined == false)
        }

        error ! new Exception("final sad panda")

        assert(errf.isDefined == true)
        assert(Await.result(errf) == OutOfRetriesException)
      }
    }
  }

  test("Client.readReliably should close on close requested") {
    new ClientReliablyHelper {
      val h = client.readReliably("foo")
      verify(rh, times(0)).close()
      h.close()
      verify(rh).close()
    }
  }

  test("ConnectedClient.read should interrupt current request on close") {
    new GlobalHelper {
      val queueName = "foo"
      val queueNameBuf = Buf.Utf8(queueName)
      val factory = mock[ServiceFactory[Command, Response]]
      val service = mock[Service[Command, Response]]
      val client = new ConnectedClient(factory)
      val open = Open(queueNameBuf, Some(Duration.Top))
      val closeAndOpen = CloseAndOpen(queueNameBuf, Some(Duration.Top))
      val abort = Abort(queueNameBuf)

      when(factory.apply()) thenReturn Future(service)
      val promise = new Promise[Response]()
      @volatile var wasInterrupted = false
      promise.setInterruptHandler { case _cause =>
        wasInterrupted = true
      }
      when(service(open)) thenReturn promise
      when(service(closeAndOpen)) thenReturn promise
      when(service(abort)) thenReturn Future(Values(Seq()))

      val rh = client.read(queueName)

      assert(wasInterrupted == false)
      rh.close()
      assert(wasInterrupted == true)
    }
  }

  test("ThriftConnectedClient.read should interrupt current trift request on close") {
    val queueName = "foo"
    val clientFactory = mock[FinagledClientFactory]
    val finagledClient = mock[FinagledClosableClient]
    val client = new ThriftConnectedClient(clientFactory, Duration.Top)

    when(clientFactory.apply()) thenReturn Future(finagledClient)
    val promise = new Promise[Seq[Item]]()

    @volatile var wasInterrupted = false
    promise.setInterruptHandler { case _cause =>
      wasInterrupted = true
    }

    when(finagledClient.get(queueName, 1, Int.MaxValue, Int.MaxValue)) thenReturn promise

    val rh = client.read(queueName)

    assert(wasInterrupted == false)
    rh.close()
    assert(wasInterrupted == true)
  }
}
