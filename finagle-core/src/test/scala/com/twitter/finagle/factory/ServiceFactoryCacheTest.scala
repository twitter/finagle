package com.twitter.finagle.factory

import com.twitter.conversions.DurationOps._
import com.twitter.finagle._
import com.twitter.util.Future
import com.twitter.util.MockTimer
import com.twitter.util.Time
import com.twitter.util.Timer
import com.twitter.util.Await
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ServiceFactoryCacheTest extends AnyFunSuite with MockitoSugar {
  trait Ctx {
    var factories: Map[Int, Int] = Map.empty
    var news: Map[Int, Int] = Map.empty

    case class SF(i: Int) extends ServiceFactory[String, String] {
      assert(!(factories contains i))
      factories += (i -> 0)
      news += (i -> (1 + news.getOrElse(i, 0)))

      def apply(conn: ClientConnection) =
        Future.value(new Service[String, String] {
          factories = factories + (i -> (factories(i) + 1))
          def apply(req: String) = Future.value(i.toString)
          override def close(deadline: Time) = {
            factories += (i -> (factories(i) - 1))
            Future.Done
          }
        })

      def close(deadline: Time) = {
        assert(factories.contains(i))
        factories -= i
        Future.Done
      }

      def status: Status = Status.Open
    }
  }

  test("cache, evict")(Time.withCurrentTimeFrozen { tc =>
    new Ctx {
      val newFactory: Int => ServiceFactory[String, String] = { i => SF(i) }
      val cache =
        new ServiceFactoryCache[Int, String, String](newFactory, Timer.Nil, maxCacheSize = 2)

      assert(factories.isEmpty)

      val s1 = Await.result(cache(1, ClientConnection.nil), 1.second)
      assert(factories == Map(1 -> 1))
      val s2 = Await.result(cache(2, ClientConnection.nil), 1.second)
      assert(factories == Map(1 -> 1, 2 -> 1))

      val s3 = Await.result(cache(3, ClientConnection.nil), 1.second)
      assert(factories == Map(1 -> 1, 2 -> 1, 3 -> 1))
      Await.result(s3.close(), 1.second)

      assert(factories == Map(1 -> 1, 2 -> 1))
      Await.result(s2.close(), 1.second)
      tc.advance(1.second)
      assert(factories == Map(1 -> 1, 2 -> 0))
      Await.result(s1.close(), 1.second)
      tc.advance(1.second)
      assert(factories == Map(1 -> 0, 2 -> 0))

      assert(news == Map(1 -> 1, 2 -> 1, 3 -> 1))

      val s3x = Await.result(cache(3, ClientConnection.nil), 1.second)

      assert(factories == Map(1 -> 0, 3 -> 1))
      assert(news == Map(1 -> 1, 2 -> 1, 3 -> 2))

      val s1x, s1y = Await.result(cache(1, ClientConnection.nil), 1.second)
      assert(factories == Map(1 -> 2, 3 -> 1))
      assert(news == Map(1 -> 1, 2 -> 1, 3 -> 2))

      val s2x = Await.result(cache(2, ClientConnection.nil), 1.second)
      assert(factories == Map(1 -> 2, 3 -> 1, 2 -> 1))
      assert(news == Map(1 -> 1, 2 -> 2, 3 -> 2))
    }
  })

  test("active eviction")(Time.withCurrentTimeFrozen { tc =>
    new Ctx {
      val newFactory: Int => ServiceFactory[String, String] = { i => SF(i) }
      val tti = 1.minute
      val timer = new MockTimer
      val cache =
        new ServiceFactoryCache[Int, String, String](newFactory, timer, maxCacheSize = 3, tti = tti)

      assert(factories.isEmpty)

      var s1 = Await.result(cache(1, ClientConnection.nil), 1.second)
      var s2 = Await.result(cache(2, ClientConnection.nil), 1.second)
      var s3 = Await.result(cache(3, ClientConnection.nil), 1.second)
      s1.close()
      s2.close()
      s3.close()

      assert(factories == Map(1 -> 0, 2 -> 0, 3 -> 0))

      tc.advance(tti / 2)
      timer.tick()

      // none purged yet
      assert(factories == Map(1 -> 0, 2 -> 0, 3 -> 0))

      // s1 and s2 stay active
      s1 = Await.result(cache(1, ClientConnection.nil), 1.second)
      s1.close()
      s2 = Await.result(cache(2, ClientConnection.nil), 1.second)
      s2.close()

      tc.advance(tti)
      timer.tick()

      assert(factories == Map(1 -> 0, 2 -> 0))

      s1 = Await.result(cache(1, ClientConnection.nil), 1.second)
      s1.close()
      tc.advance(1.second)
      s2 = Await.result(cache(2, ClientConnection.nil), 1.second)
      s2.close()
      tc.advance(1.second)

      assert(factories == Map(1 -> 0, 2 -> 0))

      tc.advance(tti)
      timer.tick()
      // Avoid expiring least idle entry (s2).
      assert(factories == Map(2 -> 0))
    }
  })

  test("close")(Time.withCurrentTimeFrozen { tc =>
    new Ctx {
      val newFactory: Int => ServiceFactory[String, String] = { i => SF(i) }
      val tti = 1.minute
      val timer = new MockTimer
      val cache =
        new ServiceFactoryCache[Int, String, String](newFactory, timer, maxCacheSize = 3, tti = tti)

      assert(factories.isEmpty)

      var s1 = Await.result(cache(1, ClientConnection.nil), 1.second)
      var s2 = Await.result(cache(2, ClientConnection.nil), 1.second)
      var s3 = Await.result(cache(3, ClientConnection.nil), 1.second)
      s1.close()

      assert(factories == Map(1 -> 0, 2 -> 1, 3 -> 1))

      tc.advance(tti)
      tc.advance(1.second)
      timer.tick()

      assert(factories == Map(2 -> 1, 3 -> 1))

      Await.result(cache.close(), 1.second)
      assert(factories.isEmpty)
    }
  })
}
