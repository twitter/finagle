package com.twitter.finagle.builder

import com.twitter.finagle._
import com.twitter.finagle.service.StatsFilter
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.integration.IntegrationBase
import com.twitter.util.{Await, Promise, Return, Future, Time}
import org.mockito.Matchers
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class ClientBuilderSpec extends SpecificationWithJUnit with IntegrationBase with Mockito {
  trait ClientBuilderHelper {
    val preparedFactory = mock[ServiceFactory[String, String]]
    val preparedServicePromise = new Promise[Service[String, String]]
    preparedFactory() returns preparedServicePromise
    preparedFactory.close(any[Time]) returns Future.Done
    preparedFactory.map(Matchers.any()) returns
    preparedFactory.asInstanceOf[ServiceFactory[Any, Nothing]]

    val m = new MockChannel
    m.codec.prepareConnFactory(any) returns preparedFactory
  }

  "ClientBuilder" should {
    "invoke prepareConnFactory on connection" in {
      new ClientBuilderHelper {
        val client = m.build()
        val requestFuture = client("123")

        there was one(m.codec).prepareConnFactory(any)
        there was one(preparedFactory)()

        requestFuture.isDefined must beFalse
        val service = mock[Service[String, String]]
        service("123") returns Future.value("321")
        service.close(any[Time]) returns Future.Done
        preparedServicePromise() = Return(service)

        there was one(service)("123")
        requestFuture.poll must beSome(Return("321"))
      }
    }

    "close properly" in {
      new ClientBuilderHelper {
        val svc = ClientBuilder().hostConnectionLimit(1).codec(m.codec).hosts("").build()
        val f = svc.close()
        f.isDefined must eventually(beTrue)
      }
    }

    "collect stats on 'tries' for retrypolicy" in {
      new ClientBuilderHelper {
        val inMemory = new InMemoryStatsReceiver
        val client = ClientBuilder()
          .name("test")
          .hostConnectionLimit(1)
          .codec(m.codec)
          .hosts(Seq(m.clientAddress))
          .retries(2) // retries == total attempts :(
          .reportTo(inMemory)
          .build()

        val service = mock[Service[String, String]]
        service("123") returns Future.exception(WriteException(new Exception()))
        service.close(any[Time]) returns Future.Done
        preparedServicePromise() = Return(service)

        val f = client("123")

        f.isDefined must beTrue
        inMemory.counters(Seq("test", "tries", "requests")) must be(1)
        inMemory.counters(Seq("test", "requests")) must be(2)
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
}
