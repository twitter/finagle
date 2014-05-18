package com.twitter.finagle.builder

import org.mockito.Mockito.{verify, when}
import org.mockito.Matchers
import org.mockito.Matchers._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.scalatest.junit.JUnitRunner
import org.scalatest.concurrent.Eventually._
import org.junit.runner.RunWith
import com.twitter.finagle.integration.IntegrationBase
import com.twitter.finagle.{WriteException, Service, ServiceFactory}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util._

@RunWith(classOf[JUnitRunner])
class ClientBuilderTest extends FunSuite with MockitoSugar with IntegrationBase {

  trait ClientBuilderHelper {
    val preparedFactory = mock[ServiceFactory[String, String]]
    val preparedServicePromise = new Promise[Service[String, String]]
    when(preparedFactory()) thenReturn preparedServicePromise
    when(preparedFactory.close(any[Time])) thenReturn Future.Done
    when(preparedFactory.map(Matchers.any())) thenReturn
      preparedFactory.asInstanceOf[ServiceFactory[Any, Nothing]]

    val m = new MockChannel
    when(m.codec.prepareConnFactory(any[ServiceFactory[String, String]])) thenReturn preparedFactory
  }

  test("ClientBuilder should invoke prepareConnFactory on connection") {
    new ClientBuilderHelper {
      val client = m.build()
      val requestFuture = client("123")

      verify(m.codec).prepareConnFactory(any[ServiceFactory[String, String]])
      verify(preparedFactory)()

      assert(!requestFuture.isDefined)
      val service = mock[Service[String, String]]
      when(service("123")) thenReturn Future.value("321")
      when(service.close(any[Time])) thenReturn Future.Done
      preparedServicePromise() = Return(service)

      verify(service)("123")
      assert(requestFuture.poll === Some(Return("321")))
    }
  }

  test("ClientBuilder should close properly") {
    new ClientBuilderHelper {
      val svc = ClientBuilder().hostConnectionLimit(1).codec(m.codec).hosts("").build()
      val f = svc.close()
      eventually {
        f.isDefined
      }
    }
  }

  test("ClientBuilder should collect stats on 'tries' for retrypolicy") {
    new ClientBuilderHelper {
      val inMemory = new InMemoryStatsReceiver
      val client = ClientBuilder()
        .name("test")
        .hostConnectionLimit(1)
        .codec(m.codec)
        .hosts(Seq(m.clientAddress))
        .retries(2) // retries === total attempts :(
        .reportTo(inMemory)
        .build()

      val service = mock[Service[String, String]]
      when(service("123")) thenReturn Future.exception(WriteException(new Exception()))
      when(service.close(any[Time])) thenReturn Future.Done
      preparedServicePromise() = Return(service)

      val f = client("123")

      assert(f.isDefined)
      assert(inMemory.counters(Seq("test", "tries", "requests")) === 1)
      assert(inMemory.counters(Seq("test", "requests")) === 2)
    }
  }


  /* TODO: Stopwatches eliminated mocking.
    "measure codec connection preparation latency" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val m = new MockChannel {
          codec.prepareConnFactory(any) answers { s =>
            val factory = s.asInstanceOf[ServiceFactory[String, String]]
            // Create a fake ServiceFactory that take time to return a dummy Service
            new ServiceFactoryProxy[String, String](factory) {
              override def apply(con: ClientConnection) = {
                timeControl.advance(500.milliseconds)
                Future.value(new Service[String, String] {
                  def apply(req: String) = Future.value(req)
                })
              }
            }
          }
        }
        val service = m.clientBuilder.build()
        timeControl.advance(100.milliseconds)
        service("blabla")

        val key = Seq(m.name, "codec_connection_preparation_latency_ms")
        val stat = m.statsReceiver.stats.get(key).get
        stat.head must be_==(500.0f)
      }
    }
*/
}
