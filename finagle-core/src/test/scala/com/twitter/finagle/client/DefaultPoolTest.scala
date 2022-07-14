package com.twitter.finagle.client

import com.twitter.finagle.Status
import com.twitter.finagle.ClientConnection
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.Return
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Time
import org.scalatest.funsuite.AnyFunSuite

class DefaultPoolTest extends AnyFunSuite {
  class MockServiceFactory extends ServiceFactory[Unit, Unit] {
    override def apply(conn: ClientConnection): Future[Service[Unit, Unit]] =
      Future.value(new MockService())

    override def close(deadline: Time): Future[Unit] = Future.Done

    def status: Status = Status.Open
  }

  class MockService extends Service[Unit, Unit] {
    @volatile var closed = false

    override def apply(unit: Unit): Future[Unit] =
      if (closed) Future.exception(new Exception) else Future.Done

    override def close(deadline: Time): Future[Unit] = {
      closed = true
      Future.Done
    }

    override def status: Status =
      if (closed) Status.Closed else Status.Open
  }

  trait DefaultPoolHelper {
    val underlying = new MockServiceFactory()
    val sr = new InMemoryStatsReceiver()
    val factory = DefaultPool[Unit, Unit](2, 3)(sr)(underlying)
  }

  test(
    "DefaultPool should be able to maintain high - low connections in the " +
      "pool, and low connection in watermark"
  ) {

    new DefaultPoolHelper {
      val c1 = Await.result(factory())
      assert(sr.gauges(Seq("pool_cached"))() == 0)
      assert(sr.gauges(Seq("pool_size"))() == 1)
      val c2 = Await.result(factory())
      assert(sr.gauges(Seq("pool_cached"))() == 0)
      assert(sr.gauges(Seq("pool_size"))() == 2)
      val c3 = Await.result(factory())
      assert(sr.gauges(Seq("pool_cached"))() == 0)
      assert(sr.gauges(Seq("pool_size"))() == 3)
      c1.close()
      assert(sr.gauges(Seq("pool_cached"))() == 1)
      assert(sr.gauges(Seq("pool_size"))() == 2)
      c2.close()
      assert(sr.gauges(Seq("pool_cached"))() == 1)
      assert(sr.gauges(Seq("pool_size"))() == 2)
      c3.close()
      assert(sr.gauges(Seq("pool_cached"))() == 1)
      assert(sr.gauges(Seq("pool_size"))() == 2)
    }
  }

  test(
    "DefaultPool should be able to reuse connections after they have been " +
      "released."
  ) {
    new DefaultPoolHelper {
      val c1 = Await.result(factory())
      val c2 = Await.result(factory())
      val c3 = Await.result(factory())
      c1.close()
      c2.close()
      c3.close()
      val c4 = Await.result(factory())
      val c5 = Await.result(factory())
      val c6 = Await.result(factory())

      // should not throw exceptions
      assert(Await.result(c4(()).liftToTry) == Return.Unit)
      assert(Await.result(c5(()).liftToTry) == Return.Unit)
      assert(Await.result(c6(()).liftToTry) == Return.Unit)
    }
  }
}
