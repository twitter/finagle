package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle.stats.{NullStatsReceiver, InMemoryStatsReceiver}
import com.twitter.finagle.{Status, MockTimer, ServiceFactory, Service, ServiceFactoryWrapper, Stack}
import com.twitter.finagle.param
import com.twitter.util._
import java.util.concurrent.TimeUnit
import org.junit.runner.RunWith
import org.mockito.Mockito.{times, verify, when}
import org.mockito.Matchers
import org.mockito.Matchers._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class FailureAccrualFactoryTest extends FunSuite with MockitoSugar {

  class Helper {
    val statsReceiver = new InMemoryStatsReceiver()
    val underlyingService = mock[Service[Int, Int]]
    when(underlyingService.close(any[Time])) thenReturn Future.Done
    when(underlyingService.status) thenReturn Status.Open
    when(underlyingService(Matchers.anyInt)) thenReturn Future.exception(new Exception)

    val underlying = mock[ServiceFactory[Int, Int]]
    when(underlying.close(any[Time])) thenReturn Future.Done
    when(underlying.status) thenReturn Status.Open
    when(underlying()) thenReturn Future.value(underlyingService)

    val timer = new MockTimer
    val factory = new FailureAccrualFactory[Int, Int](
      underlying, 3, () => 10.seconds, timer, statsReceiver, "test")
    val service = Await.result(factory())
    verify(underlying)()
  }

  test("a failing service should become unavailable") {
    val h = new Helper
    import h._

    Time.withCurrentTimeFrozen { timeControl =>
      intercept[Exception] {
        Await.result(service(123))
      }
      intercept[Exception] {
        Await.result(service(123))
      }
      assert(factory.isAvailable)
      assert(service.isAvailable)

      // Now fail:
      intercept[Exception] {
        Await.result(service(123))
      }
      assert(statsReceiver.counters.get(List("removals")) === Some(1))
      assert(!factory.isAvailable)
      assert(!service.isAvailable)

      verify(underlyingService, times(3))(123)
    }
  }

  test("a failing service should be revived (for one request) after the markDeadFor duration") {
    val h = new Helper
    import h._

    Time.withCurrentTimeFrozen { timeControl =>
      intercept[Exception] {
        Await.result(service(123))
      }
      intercept[Exception] {
        Await.result(service(123))
      }
      intercept[Exception] {
        Await.result(service(123))
      }
      assert(statsReceiver.counters.get(List("removals")) === Some(1))
      assert(!factory.isAvailable)
      assert(!service.isAvailable)

      timeControl.advance(10.seconds)
      timer.tick()

      // Healthy again!
      assert(statsReceiver.counters.get(List("removals")) === Some(1))
      assert(statsReceiver.counters.get(List("revivals")) === Some(1))
      assert(factory.isAvailable)
      assert(service.isAvailable)

      // But after one bad dispatch, mark it again unhealthy.
      intercept[Exception] {
        Await.result(service(123))
      }

      assert(statsReceiver.counters.get(List("removals")) === Some(2))
      assert(!factory.isAvailable)
      assert(!service.isAvailable)
    }
  }

  test("a failing factory should be busy; done when revived") {
    Time.withCurrentTimeFrozen { tc =>
      val h = new Helper
      import h._

      assert(factory.status === Status.Open)
      intercept[Exception] {
        Await.result(service(123))
      }
      intercept[Exception] {
        Await.result(service(123))
      }
      assert(factory.status === Status.Open)
      intercept[Exception] {
        Await.result(service(123))
      }

      assert(factory.status == Status.Busy)

      tc.advance(10.seconds)
      timer.tick()

      assert(factory.status === Status.Open)
    }
  }

  test("a failing service should reset failure counters after an individual success") {
    val h = new Helper
    import h._

    Time.withCurrentTimeFrozen { timeControl =>
      intercept[Exception] {
        Await.result(service(123))
      }
      intercept[Exception] {
        Await.result(service(123))
      }
      intercept[Exception] {
        Await.result(service(123))
      }
      assert(statsReceiver.counters.get(List("removals")) === Some(1))
      assert(!factory.isAvailable)
      assert(!service.isAvailable)

      timeControl.advance(10.seconds)
      timer.tick()

      // Healthy again!
      assert(statsReceiver.counters.get(List("revivals")) === Some(1))
      assert(statsReceiver.counters.get(List("removals")) === Some(1))
      assert(factory.isAvailable)
      assert(service.isAvailable)

      when(underlyingService(123)) thenReturn Future.value(321)

      // A good dispatch!
      assert(statsReceiver.counters.get(List("revivals")) === Some(1))
      assert(statsReceiver.counters.get(List("removals")) === Some(1))
      assert(Await.result(service(123)) === 321)

      assert(factory.isAvailable)
      assert(service.isAvailable)

      // Counts are now reset.
      when(underlyingService(123)) thenReturn Future.exception(new Exception)
      intercept[Exception] {
        Await.result(service(123))
      }
      assert(statsReceiver.counters.get(List("revivals")) === Some(1))
      assert(statsReceiver.counters.get(List("removals")) === Some(1))
      assert(factory.isAvailable)
      assert(service.isAvailable)
      intercept[Exception] {
        Await.result(service(123))
      }
      assert(factory.isAvailable)
      assert(service.isAvailable)
      intercept[Exception] {
        Await.result(service(123))
      }
      assert(statsReceiver.counters.get(List("revivals")) === Some(1))
      assert(statsReceiver.counters.get(List("removals")) === Some(2))
      assert(!factory.isAvailable)
      assert(!service.isAvailable)
    }
  }

  class HealthyServiceHelper {
    val statsReceiver = new InMemoryStatsReceiver()
    val underlyingService = mock[Service[Int, Int]]
    when(underlyingService.close(any[Time])) thenReturn Future.Done
    when(underlyingService.status) thenReturn Status.Open
    when(underlyingService(Matchers.anyInt)) thenReturn Future.value(321)

    val underlying = mock[ServiceFactory[Int, Int]]
    when(underlying.close(any[Time])) thenReturn Future.Done
    when(underlying.status) thenReturn Status.Open
    when(underlying()) thenReturn Future.value(underlyingService)

    val factory = new FailureAccrualFactory[Int, Int](
      underlying, 3, () => 10.seconds, new MockTimer, statsReceiver, "test")
    val service = Await.result(factory())
    verify(underlying)()
  }

  test("a healthy service should [service] pass through underlying availability") {
    val h = new HealthyServiceHelper
    import h._

    assert(service.isAvailable)
    when(underlyingService.status) thenReturn Status.Closed
    assert(!service.isAvailable)
  }

  test("a healthy service should [factory] pass through underlying availability") {
    val h = new HealthyServiceHelper
    import h._

    assert(factory.isAvailable)
    assert(service.isAvailable)
    when(underlying.status) thenReturn Status.Closed
    assert(!factory.isAvailable)

    // This propagates to the service as well.
    assert(!service.isAvailable)

    when(underlying.status) thenReturn Status.Busy

    assert(service.status === Status.Busy)
  }

  class BrokenFactoryHelper {
    val statsReceiver = new InMemoryStatsReceiver()
    val underlying = mock[ServiceFactory[Int, Int]]
    when(underlying.close(any[Time])) thenReturn Future.Done
    when(underlying.status) thenReturn Status.Open
    val exc = new Exception("i broked :-(")
    when(underlying()) thenReturn Future.exception(exc)
    val factory = new FailureAccrualFactory[Int, Int](
      underlying, 3, () => 10.seconds, new MockTimer, statsReceiver, "test")
  }

  test("a broken factory should fail after the given number of tries") {
    val h = new BrokenFactoryHelper
    import h._

    Time.withCurrentTimeFrozen { timeControl =>
      assert(factory.isAvailable)
      intercept[Exception] {
        Await.result(factory())
      }
      assert(factory.isAvailable)
      intercept[Exception] {
        Await.result(factory())
      }
      assert(factory.isAvailable)
      intercept[Exception] {
        Await.result(factory())
      }
      assert(!factory.isAvailable)
    }
  }

  class CustomizedFactory {
    class CustomizedFailureAccrualFactory(
      underlying: ServiceFactory[Int, Int],
      numFailures: Int,
      markDeadFor: Duration,
      timer: Timer,
      label: String
    ) extends FailureAccrualFactory[Int, Int](underlying, numFailures, () => markDeadFor, timer, NullStatsReceiver, label) {
      override def isSuccess(response: Try[Int]): Boolean = {
        response match {
          case Throw(_) => false
          case Return(x) => x != 321
        }
      }
    }

    val underlyingService = mock[Service[Int, Int]]
    when(underlyingService.close(any[Time])) thenReturn Future.Done
    when(underlyingService.status) thenReturn Status.Open
    when(underlyingService(Matchers.anyInt)) thenReturn Future.value(321)

    val underlying = mock[ServiceFactory[Int, Int]]
    when(underlying.close(any[Time])) thenReturn Future.Done
    when(underlying.status) thenReturn Status.Open
    when(underlying()) thenReturn Future.value(underlyingService)

    val timer = new MockTimer
    val factory = new CustomizedFailureAccrualFactory(
      underlying, 3, 10.seconds, timer, "test")
    val service = Await.result(factory())
    verify(underlying)()
  }

  test("a customized factory should become unavailable") {
    val h = new CustomizedFactory
    import h._

    Time.withCurrentTimeFrozen { timeControl =>
      assert(Await.result(service(123)) === 321)
      assert(Await.result(service(123)) === 321)
      assert(factory.isAvailable)
      assert(service.isAvailable)

      // Now fail:
      assert(Await.result(service(123)) === 321)
      assert(!service.isAvailable)

      verify(underlyingService, times(3))(123)
    }
  }

  test("perturbs") {
    val perturbation = 0.2f
    val duration = 1.seconds
    val rand = new Random(1)
    for (_ <- 1 to 50) {
      val d = FailureAccrualFactory.perturb(duration, perturbation, rand)()
      val diff = d.diff(duration).inUnit(TimeUnit.MILLISECONDS)
      assert(diff >= 0)
      assert(diff < 200)
    }
  }

  test("param") {
    import FailureAccrualFactory._

    val p1: Param = Param(42, () => Duration.fromSeconds(10))
    val p2: Param = Replaced(_ => ServiceFactoryWrapper.identity)
    val p3: Param = Disabled

    assert((p1 match { case Param.Configured(x, _) => x }) == 42)
    assert((p2 match { case Param.Replaced(f) => f(null) }) == ServiceFactoryWrapper.identity)
    assert(p3 match { case Disabled => true })

    val ps1: Stack.Params = Stack.Params.empty + p1
    assert(ps1.contains[Param])
    assert((ps1[Param] match { case Param.Configured(x, _) => x }) == 42)

    val ps2: Stack.Params = Stack.Params.empty + p2 + p1
    assert(ps2.contains[Param])
    assert((ps2[Param] match { case Param.Configured(x, _) => x }) == 42)

    val ps3: Stack.Params = Stack.Params.empty + p1 + p2 + p3
    assert(ps3.contains[Param])
    assert(ps3[Param] match { case Disabled => true })
  }

  test("module") {
    val h = new Helper
    val s: Stack[ServiceFactory[Int, Int]] =
      FailureAccrualFactory.module[Int, Int].toStack(Stack.Leaf(Stack.Role("Service"), h.underlying))

    val ps: Stack.Params = Stack.Params.empty + param.Stats(h.statsReceiver)

    // disabled
    Await.ready(s.make(ps + FailureAccrualFactory.Disabled).toService(10))
    assert(!h.statsReceiver.counters.contains(Seq("failure_accrual", "removals")))

    // replaced
    Await.ready(s.make(ps + FailureAccrualFactory.Replaced(ServiceFactoryWrapper.identity)).toService(10))
    assert(!h.statsReceiver.counters.contains(Seq("failure_accrual", "removals")))

    // configured
    Await.ready(s.make(ps + FailureAccrualFactory.Param(1, Duration.Top)).toService(10))
    assert(h.statsReceiver.counters.contains(Seq("failure_accrual", "removals")))
  }
}
