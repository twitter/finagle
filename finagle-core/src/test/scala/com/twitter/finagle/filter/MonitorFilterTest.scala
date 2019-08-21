package com.twitter.finagle.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.client.utils.StringClient
import com.twitter.finagle.server.utils.StringServer
import com.twitter.util._
import java.net.{InetAddress, InetSocketAddress}
import java.util.logging.{Level, Logger, StreamHandler}
import org.mockito.Matchers._
import org.mockito.Mockito.{times, verify, when}
import org.mockito.{Matchers, Mockito}
import org.scalatest.FunSuite
import org.scalatestplus.mockito.MockitoSugar

class MonitorFilterTest extends FunSuite with MockitoSugar {

  class MockMonitor extends Monitor {
    def handle(cause: Throwable) = false
  }

  class MonitorFilterHelper {
    val monitor = Mockito.spy(new MockMonitor)
    val underlying = mock[Service[Int, Int]]
    when(underlying.close(any[Time])) thenReturn Future.Done
    val reply = new Promise[Int]
    when(underlying(any[Int])) thenReturn reply
    val service = new MonitorFilter(monitor) andThen underlying
    val exc = new RuntimeException
  }

  test("MonitorFilter should report Future.exception") {
    val h = new MonitorFilterHelper
    import h._

    val f = service(123)
    assert(f.poll == None)

    reply() = Throw(exc)
    assert(f.poll == Some(Throw(exc)))
    verify(monitor).handle(exc)
  }

  test("MonitorFilter should report raw service exception") {
    val h = new MonitorFilterHelper
    import h._

    when(underlying(any[Int])) thenThrow exc

    val f = service(123)
    assert(f.poll == Some(Throw(exc)))
    verify(monitor).handle(exc)
  }

  test("MonitorFilter should not fail on exceptions thrown in callbacks") {
    var handled = false
    val monitor = Monitor.mk {
      case _ =>
        handled = true
        true
    }
    val p1 = Promise[Unit]
    val p2 = Promise[Int]
    val svc = Service.mk[Int, Int] { num: Int =>
      p1.onSuccess { _ =>
        throw new Exception("boom!")
      }
      p1.before(p2)
    }
    val filter = new MonitorFilter[Int, Int](monitor)
    val filteredSvc = filter.andThen(svc)

    val f = filteredSvc(0)
    p1.setDone()
    assert(handled)
    assert(!f.isDefined)
    p2.setValue(1)
    assert(Await.result(f, 2.seconds) == 1)
  }

  class MockSourcedException(underlying: Throwable, name: String)
      extends RuntimeException(underlying)
      with SourcedException {
    def this(name: String) = this(null, name)
    serviceName = name
  }

  class Helper {
    val monitor = Mockito.spy(new MockMonitor)
    val inner = new MockSourcedException("FakeService1")
    val outer = new MockSourcedException(inner, SourcedException.UnspecifiedServiceName)

    val mockLogger = Mockito.spy(Logger.getLogger("MockServer"))
    // add handler to redirect and mute output, so that it doesn't show up in the console during a test run.
    mockLogger.setUseParentHandlers(false)
    mockLogger.addHandler(new StreamHandler())
  }

  test("MonitorFilter should when attached to a server, report source for sourced exceptions") {
    val h = new Helper
    import h._

    val address = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
    val service = mock[Service[String, String]]
    when(service.close(any[Time])) thenReturn Future.Done
    val server = ServerBuilder()
      .stack(StringServer.server)
      .name("FakeService2")
      .bindTo(address)
      .monitor((_, _) => monitor)
      .logger(mockLogger)
      .build(service)

    // We cannot mock "service" directly, because we are testing an internal filter defined in the ServerBuilder
    // that sits on top of "service". Therefore we need to create a client to initiates the requests.
    val client = ClientBuilder()
      .stack(StringClient.client)
      .hosts(Seq(server.boundAddress.asInstanceOf[InetSocketAddress]))
      .hostConnectionLimit(1)
      .build()

    when(service(any[String])) thenThrow outer // make server service throw the mock exception

    try {
      val f = Await.result(client("123"))
    } catch {
      case e: ChannelException => // deliberately empty. Server exception comes back as ChannelClosedException
    }

    verify(monitor, times(0)).handle(inner)
    verify(monitor).handle(outer)
    verify(mockLogger).log(
      Matchers.eq(Level.SEVERE),
      Matchers.eq(
        "The 'FakeService2' service FakeService2 on behalf of FakeService1 threw an exception"
      ),
      Matchers.eq(outer)
    )

    // need to properly close the client and the server, otherwise they will prevent ExitGuard from exiting and interfere with ExitGuardTest
    Await.ready(client.close(), 1.second)
    Await.ready(server.close(), 1.second)
  }

  test("MonitorFilter should when attached to a client, report source for sourced exceptions") {
    val h = new Helper
    import h._

    val mockService = new Service[String, String] {
      def apply(request: String): Future[String] = Future.exception(outer)
    }

    val client = ClientBuilder()
      .stack(StringClient.client.withEndpoint(mockService))
      .monitor(_ => monitor)
      .logger(mockLogger)
      .hosts(Seq(new InetSocketAddress(0)))
      .build()

    val response = Await.result(client("123").liftToTry, 10.seconds)

    assert(response == Throw(outer))
    verify(monitor, times(0)).handle(inner)
    verify(monitor).handle(outer)
  }
}
