package com.twitter.finagle.builder

import com.twitter.finagle._
import com.twitter.finagle.integration.IntegrationBase
import com.twitter.util.{Promise, Return, Future}
import org.mockito.Matchers
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class ClientBuilderSpec extends SpecificationWithJUnit with IntegrationBase with Mockito {
  "ClientBuilder" should {
    "invoke prepareConnFactory on connection" in {
      val preparedFactory = mock[ServiceFactory[String, String]]
      val preparedServicePromise = new Promise[Service[String, String]]
      preparedFactory() returns preparedServicePromise
      preparedFactory.close(any) returns Future.Done
      preparedFactory.map(Matchers.any()) returns
        preparedFactory.asInstanceOf[ServiceFactory[Any, Nothing]]

      val m = new MockChannel
      m.codec.prepareConnFactory(any) returns preparedFactory

      // Client
      val client = m.build()
      val requestFuture = client("123")

      there was one(m.codec).prepareConnFactory(any)
      there was one(preparedFactory)()

      requestFuture.isDefined must beFalse
      val service = mock[Service[String, String]]
      service("123") returns Future.value("321")
      service.close(any) returns Future.Done
      preparedServicePromise() = Return(service)
      there was one(service)("123")
      requestFuture.poll must beSome(Return("321"))
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
