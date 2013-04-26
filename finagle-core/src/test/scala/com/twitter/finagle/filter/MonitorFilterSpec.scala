package com.twitter.finagle.filter

import com.twitter.finagle._
import com.twitter.finagle.builder.{ClientBuilder, ServerBuilder}
import com.twitter.finagle.integration.{IntegrationBase, StringCodec}
import com.twitter.util.{Await, Future, Monitor, Promise, Return, Throw}
import java.net.InetSocketAddress
import java.util.logging.{Level, Logger, StreamHandler}
import org.mockito.Matchers
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class MonitorFilterSpec extends SpecificationWithJUnit with IntegrationBase with Mockito {
  class MockMonitor extends Monitor {
    def handle(cause: Throwable) = false
  }
  val monitor = spy(new MockMonitor)

  "MonitorFilter" should {
    val monitor = spy(new MockMonitor)
    val underlying = mock[Service[Int, Int]]
    underlying.close(any) returns Future.Done
    val reply = new Promise[Int]
    underlying(any) returns reply
    val service = new MonitorFilter(monitor) andThen underlying
    val exc = new RuntimeException

    "report Future.exception" in {
      val f = service(123)
      f.poll must beNone

      reply() = Throw(exc)
      f.poll must beSome(Throw(exc))
      there was one(monitor).handle(exc)
    }

    "report raw service exception" in {
      underlying(any) throws exc
      val f = service(123)
      f.poll must beSome(Throw(exc))
      there was one(monitor).handle(exc)
    }
  }


  class MockSourcedException(underlying: Throwable, name: String)
    extends RuntimeException(underlying) with SourcedException {
    def this(name:String) = this(null, name)
    serviceName = name
  }



  "MonitorFilter" should {
    val inner = new MockSourcedException("FakeService1")
    val outer = new MockSourcedException(inner, "FakeService2")

    val mockLogger = spy(Logger.getLogger("MockServer"))
    // add handler to redirect and mute output, so that it doesn't show up in the console during a test run.
    mockLogger.setUseParentHandlers(false)
    mockLogger.addHandler(new StreamHandler())


    "when attached to a server, report source for sourced exceptions" in {
      val address = new InetSocketAddress(0)
      val service = mock[Service[String, String]]
      service.close(any) returns Future.Done
      val server = ServerBuilder()
        .codec(StringCodec)
        .name("MockServer")
        .bindTo(address)
        .monitor((_, _) => monitor)
        .logger(mockLogger)
        .build(service)

      // We cannot mock "service" directly, because we are testing an internal filter defined in the ServerBuilder
      // that sits on top of "service". Therefore we need to create a client to initiates the requests.
      val client = ClientBuilder()
        .codec(StringCodec)
        .hosts(Seq(server.localAddress))
        .hostConnectionLimit(1)
        .build()

      service(any) throws outer // make server service throw the mock exception

      try {
        val f = Await.result(client("123"))
      } catch {
        case e: ChannelException => // deliberately empty. Server exception comes back as ChannelClosedException
      }

      there was no(monitor).handle(inner)
      there was one(monitor).handle(outer)
      there was one(mockLogger).log(
        Matchers.eq(Level.SEVERE),
        Matchers.eq("A server service FakeService2 on behalf of FakeService1 threw an exception"),
        Matchers.eq(outer))
    }

    "when attached to a client, report source for sourced exceptions" in {
      // mock channel to mock the service provided by a server
      val preparedFactory = mock[ServiceFactory[String, String]]
      val preparedServicePromise = new Promise[Service[String, String]]
      preparedFactory() returns preparedServicePromise
      preparedFactory.close(any) returns Future.Done
      preparedFactory.map(Matchers.any()) returns
        preparedFactory.asInstanceOf[ServiceFactory[Any, Nothing]]

      val m = new MockChannel
      m.codec.prepareConnFactory(any) returns preparedFactory

      val client = m.clientBuilder
        .monitor(_ => monitor)
        .logger(mockLogger)
        .build()

      val requestFuture = client("123")

      requestFuture.isDefined must beFalse
      val mockService = new Service[String, String] {
        def apply(request: String): Future[String] = Future.exception(outer)
      }

      preparedServicePromise() = Return(mockService)
      requestFuture.poll must beSome(Throw(outer))

      there was no(monitor).handle(inner)
      there was one(monitor).handle(outer)
    }
  }
}
