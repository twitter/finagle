package com.twitter.finagle.postgresql

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.postgresql.Client.Expect
import com.twitter.finagle.postgresql.DelayedReleaseServiceSpec.MockService
import com.twitter.io.Reader
import com.twitter.io.ReaderDiscardedException
import com.twitter.util._
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ClientSpec extends AnyWordSpec with Matchers {

  "Client" should {

    "succeed streaming readers with timeout" in {
      val svc = new MockService(2)
      val fact = ServiceFactory.constant(svc)

      val mockTimer = new MockTimer()

      val client = Client(fact, () => 1.second)(mockTimer)

      Time.withCurrentTimeFrozen { tc =>
        val fut = client
          .query("test")
          .flatMap(Expect.ResultSet)

        val rs = Await.result(fut)
        tc.advance(500.millisecond)
        mockTimer.tick()

        val rows = Await.result(Reader.readAllItems(rs.rows))
        rows.size must be(2)
      }
    }

    "fail streaming readers after timeout" in {
      val svc = new MockService(2)
      val fact = ServiceFactory.constant(svc)

      val mockTimer = new MockTimer()

      val client = Client(fact, () => 1.second)(mockTimer)

      Time.withCurrentTimeFrozen { tc =>
        val fut = client
          .query("test")
          .flatMap(Expect.ResultSet)

        val rs = Await.result(fut)

        tc.advance(500.milliseconds)
        mockTimer.tick()

        Await.result(rs.rows.read())

        tc.advance(1.second)
        mockTimer.tick()

        a[ReaderDiscardedException] should be thrownBy Await.result(rs.rows.read())
      }
    }

    "fail non-streaming requests after timeout" in {
      val slowSvc = Service.mk[Request, Response](_ => Future.never)
      val fact = ServiceFactory.constant(slowSvc)

      val mockTimer = new MockTimer()

      val client = Client(fact, () => 1.second)(mockTimer)

      Time.withCurrentTimeFrozen { tc =>
        val fut = client.query("test")
        mockTimer.tick()

        tc.advance(2.seconds)
        mockTimer.tick()

        a[TimeoutException] should be thrownBy Await.result(fut)
      }
    }
  }
}
