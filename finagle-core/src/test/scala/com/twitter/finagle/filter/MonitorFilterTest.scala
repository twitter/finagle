package com.twitter.finagle.filter

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.mock.MockitoSugar
import org.mockito.{Matchers, Mockito}
import org.mockito.Matchers._
import org.mockito.Mockito.{times, verify, when}
import com.twitter.finagle.{ServiceFactory, ChannelException, SourcedException, Service, Status}
import com.twitter.finagle.integration.{StringCodec, IntegrationBase}
import com.twitter.util._
import java.util.logging.{Level, StreamHandler, Logger}
import java.net.{InetAddress, InetSocketAddress}
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}

@RunWith(classOf[JUnitRunner])
class MonitorFilterTest extends FunSuite with MockitoSugar with IntegrationBase {

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

  class MockSourcedException(underlying: Throwable, name: String)
    extends RuntimeException(underlying) with SourcedException {
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
      .codec(StringCodec)
      .name("FakeService2")
      .bindTo(address)
      .monitor((_, _) => monitor)
      .logger(mockLogger)
      .build(service)

    // We cannot mock "service" directly, because we are testing an internal filter defined in the ServerBuilder
    // that sits on top of "service". Therefore we need to create a client to initiates the requests.
    val client = ClientBuilder()
      .codec(StringCodec)
      .hosts(Seq(server.boundAddress))
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
      Matchers.eq("A server service FakeService2 on behalf of FakeService1 threw an exception"),
      Matchers.eq(outer))
  }

  test("MonitorFilter should when attached to a client, report source for sourced exceptions") {
    val h = new Helper
    import h._

    // mock channel to mock the service provided by a server
    val preparedFactory = mock[ServiceFactory[String, String]]
    val preparedServicePromise = new Promise[Service[String, String]]
    when(preparedFactory()) thenReturn preparedServicePromise
    when(preparedFactory.close(any[Time])) thenReturn Future.Done
    when(preparedFactory.map(Matchers.any())) thenReturn
      preparedFactory.asInstanceOf[ServiceFactory[Any, Nothing]]
    when(preparedFactory.status) thenReturn(Status.Open)

    val m = new MockChannel
    when(m.codec.prepareConnFactory(any[ServiceFactory[String, String]])) thenReturn preparedFactory

    val client = m.clientBuilder
      .monitor(_ => monitor)
      .logger(mockLogger)
      .build()

    val requestFuture = client("123")

    assert(!requestFuture.isDefined)
    val mockService = new Service[String, String] {
      def apply(request: String): Future[String] = Future.exception(outer)
    }

    preparedServicePromise() = Return(mockService)
    assert(requestFuture.poll == Some(Throw(outer)))

    verify(monitor, times(0)).handle(inner)
    verify(monitor).handle(outer)
  }


}
