package com.twitter.finagle.netty3.channel

import com.twitter.finagle.{ClientConnection, Service, ServiceFactory}
import com.twitter.util.TimeConversions._
import com.twitter.util.{Await, Future, Promise, Time}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.{times, verify, when}
import org.mockito.Mockito
import org.mockito.Matchers._
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

@RunWith(classOf[JUnitRunner])
class IdleConnectionFilterTest extends FunSuite with MockitoSugar {

  class ChannelHelper {
    val service = mock[Service[String, String]]
    when(service.close(any)) thenReturn Future.Done
    val underlying = ServiceFactory.const(service)

    val threshold = OpenConnectionsThresholds(2, 4, 1.second)
    val filter = new IdleConnectionFilter(underlying, threshold)

    def open(filter: IdleConnectionFilter[_, _]) = {
      val c = mock[ClientConnection]
      val closeFuture = new Promise[Unit]
      when(c.onClose) thenReturn closeFuture

      when(c.close()) thenAnswer {
        new Answer[Future[Unit]] {
          override def answer(invocation: InvocationOnMock): Future[Unit] = {
            closeFuture.setDone()
            closeFuture
          }
        }

      }
      filter(c)
      (c, closeFuture)
    }
  }

  test("IdleConnectionFilter should count connections") {
    val h = new ChannelHelper
    import h._

    assert(filter.openConnections == 0)
    val (_, closeFuture) = open(filter)
    assert(filter.openConnections == 1)
    closeFuture.setDone()
    assert(filter.openConnections == 0)
  }

  test("IdleConnectionFilter should refuse connection if above highWaterMark") {
    val h = new ChannelHelper
    import h._

    assert(filter.openConnections == 0)
    val closeFutures = (1 to threshold.highWaterMark) map { _ =>
      val (_, closeFuture) = open(filter)
      closeFuture
    }
    assert(filter.openConnections == threshold.highWaterMark)
    open(filter)
    assert(filter.openConnections == threshold.highWaterMark)

    closeFutures foreach {
      _.setDone()
    }
    assert(filter.openConnections == 0)
  }

  test("IdleConnectionFilter should try to close an idle connection if above lowerWaterMark") {
    val h = new ChannelHelper
    import h._

    val spyFilter = Mockito.spy(new IdleConnectionFilter(underlying, threshold))

    assert(spyFilter.openConnections == 0)
    (1 to threshold.lowWaterMark) map { _ =>
      open(spyFilter)
    }
    assert(spyFilter.openConnections == threshold.lowWaterMark)

    // open must try to close an idle connection
    open(spyFilter)
    verify(spyFilter, times(1)).closeIdleConnections()
  }

  test("IdleConnectionFilter should don't close connections not yet answered by the server (long processing requests)") {
    val h = new ChannelHelper
    import h._

    var t = Time.now
    Time.withTimeFunction(t) { _ =>
      val service = new Service[String, String] {
        def apply(req: String): Future[String] = new Promise[String]
      }
      val underlying = ServiceFactory.const(service)
      val spyFilter = Mockito.spy(new IdleConnectionFilter(underlying, threshold))
      assert(spyFilter.openConnections == 0)
      (1 to threshold.highWaterMark) map { _ =>
        val (c, _) = open(spyFilter)
        spyFilter.filterFactory(c)("titi", service)
      }
      assert(spyFilter.openConnections == threshold.highWaterMark)

      // wait a long time
      t += threshold.idleTimeout * 3

      val c = mock[ClientConnection]
      val closeFuture = new Promise[Unit]
      when(c.onClose) thenReturn closeFuture
      /* same pb as before*/
      when(c.close()) thenAnswer {
        new Answer[Future[Unit]] {
          override def answer(invocation: InvocationOnMock): Future[Unit] = {
            closeFuture.setDone()
            closeFuture
          }
        }
      }
      spyFilter(c)

      verify(c, times(1)).close()
      assert(spyFilter.openConnections == threshold.highWaterMark)
    }
  }

  test("IdleConnectionFilter should close an idle connection to accept a new one") {
    val h = new ChannelHelper
    import h._

    var t = Time.now
    Time.withTimeFunction(t) { _ =>
      val responses = collection.mutable.HashSet.empty[Promise[String]]
      val service = new Service[String, String] {
        def apply(req: String): Future[String] = {
          val p = new Promise[String]
          responses += p
          p
        }
      }
      val underlying = ServiceFactory.const(service)
      val spyFilter = Mockito.spy(new IdleConnectionFilter(underlying, threshold))

      // Open all connections
      (1 to threshold.highWaterMark) map { _ =>
        val (c, _) = open(spyFilter)
        spyFilter.filterFactory(c)("titi", Await.result(underlying(c)))
      }

      // Simulate response from the server
      responses foreach {
        f => f.setValue("toto")
      }

      // wait a long time
      t += threshold.idleTimeout * 3

      val c = mock[ClientConnection]
      val closeFuture = new Promise[Unit]
      when(c.onClose) thenReturn closeFuture
      spyFilter(c)

      verify(c, times(0)).close()
    }
  }
}
