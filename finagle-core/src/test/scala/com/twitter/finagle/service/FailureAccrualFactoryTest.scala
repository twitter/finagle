package com.twitter.finagle.service

import com.twitter.conversions.time._
import com.twitter.finagle.stats.{NullStatsReceiver, InMemoryStatsReceiver}
import com.twitter.finagle.{Status, ServiceFactory, Service, ServiceFactoryWrapper, Stack}
import com.twitter.finagle.param
import com.twitter.finagle.service.exp.FailureAccrualPolicy
import com.twitter.util._
import java.util.concurrent.TimeUnit
import org.junit.runner.RunWith
import org.mockito.Mockito.{times, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.mockito.Matchers
import org.mockito.Matchers._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class FailureAccrualFactoryTest extends FunSuite with MockitoSugar {

  val markDeadFor = Backoff.equalJittered(5.seconds, 60.seconds)
  val markDeadForList = markDeadFor.take(6)

  def consecutiveFailures = FailureAccrualPolicy.consecutiveFailures(3, markDeadFor)

  class Helper(failureAccrualPolicy: FailureAccrualPolicy) {
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
      underlying, failureAccrualPolicy, timer, statsReceiver, "test")
    val service = Await.result(factory())
    verify(underlying)()
  }

  test("a failing service should become unavailable") {
    val h = new Helper(consecutiveFailures)
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
      assert(statsReceiver.counters.get(List("removals")) == Some(1))
      assert(!factory.isAvailable)
      assert(!service.isAvailable)

      verify(underlyingService, times(3))(123)
    }
  }

  test("a failing service should enter the probing state after the markDeadFor duration") {
    val h = new Helper(consecutiveFailures)
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
      assert(statsReceiver.counters.get(List("removals")) == Some(1))
      assert(!factory.isAvailable)
      assert(!service.isAvailable)

      timeControl.advance(10.seconds)
      timer.tick()

      // Probing, not revived yet.
      assert(statsReceiver.counters.get(List("removals")) == Some(1))
      assert(statsReceiver.counters.get(List("revivals")) == None)

      assert(factory.isAvailable)
      assert(service.isAvailable)

      // But after one bad dispatch, mark it again unhealthy.
      intercept[Exception] {
        Await.result(service(123))
      }

      assert(statsReceiver.counters.get(List("removals")) == Some(2))
      assert(!factory.isAvailable)
      assert(!service.isAvailable)

    }
  }

  test("a failing service should be revived on a backoff mechanism by default") {
    val h = new Helper(consecutiveFailures)
    import h._

    Time.withCurrentTimeFrozen { timeControl =>

      when(underlyingService(456)) thenReturn Future.value(654)

      // 3 failures must occur before the service is initially removed,
      // then one failure after each re-instating
      intercept[Exception] {
        Await.result(service(123))
      }
      intercept[Exception] {
        Await.result(service(123))
      }

      for (i <- 0 until markDeadForList.length) {
        // After another failure, the service should be unavailable
        intercept[Exception] {
          Await.result(service(123))
        }

        assert(statsReceiver.counters.get(List("removals")) == Some(i + 1))
        assert(!factory.isAvailable)
        assert(!service.isAvailable)

        // Make sure the backoff follows the pattern above; after another
        // markDeadForList(i) - 1 seconds it should still be unavailable
        timeControl.advance(markDeadForList(i) - 1.second)
        timer.tick()

        assert(statsReceiver.counters.get(List("removals")) == Some(i + 1))
        assert(!factory.isAvailable)
        assert(!service.isAvailable)

        // Now advance to + markDeadForList(i) seconds past marking dead, to equal the
        // backoff time
        timeControl.advance(1.second)
        timer.tick()

        // The service should be available for a probe
        assert(statsReceiver.counters.get(List("removals")) == Some(i + 1))
        assert(statsReceiver.counters.get(List("revivals")) == None)
        assert(factory.isAvailable)
        assert(service.isAvailable)
      }
    }
  }

  test("backoff should be 5 minutes when stream runs out") {
    val markDeadFor = Backoff.equalJittered(5.seconds, 60.seconds) take 3

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
      underlying, FailureAccrualPolicy.consecutiveFailures(3, markDeadFor), timer, statsReceiver, "test")
    val service = Await.result(factory())
    verify(underlying)()

    Time.withCurrentTimeFrozen { timeControl =>

      // 3 failures must occur before the service is initially removed,
      // then one failure after each re-instating
      intercept[Exception] {
        Await.result(service(123))
      }
      intercept[Exception] {
        Await.result(service(123))
      }

      for (i <- 0 until markDeadFor.length) {
        // After another failure, the service should be unavailable
        intercept[Exception] {
          Await.result(service(123))
        }

        assert(statsReceiver.counters.get(List("removals")) == Some(i + 1))
        assert(!factory.isAvailable)
        assert(!service.isAvailable)

        // Make sure the backoff follows the pattern above; after another
        // markDeadForList(i) - 1 seconds it should still be unavailable
        timeControl.advance(markDeadFor(i) - 1.second)
        timer.tick()

        assert(statsReceiver.counters.get(List("removals")) == Some(i + 1))
        assert(!factory.isAvailable)
        assert(!service.isAvailable)

        // Now advance to + markDeadForList(i) seconds past marking dead, to equal the
        // backoff time
        timeControl.advance(1.second)
        timer.tick()

        // The service should be available for a probe
        assert(statsReceiver.counters.get(List("removals")) == Some(i + 1))
        assert(statsReceiver.counters.get(List("revivals")) == None)
        assert(factory.isAvailable)
        assert(service.isAvailable)
      }

      intercept[Exception] {
        Await.result(service(123))
      }

      // The stream of backoffs has run out, so we should be using 300 seconds
      // as the default.
      assert(!factory.isAvailable)
      assert(!service.isAvailable)

      timeControl.advance(300.seconds - 1.second)
      timer.tick()

      assert(!factory.isAvailable)
      assert(!service.isAvailable)

      timeControl.advance(1.second)
      timer.tick()

      assert(factory.isAvailable)
      assert(service.isAvailable)
    }
  }

  test("backoff time should be reset after a success") {
    val h = new Helper(consecutiveFailures)
    import h._

    Time.withCurrentTimeFrozen { timeControl =>

      when(underlyingService(456)) thenReturn Future.value(654)

      // 3 failures must occur before the service is initially removed,
      // then one failure after each probing
      for (i <- 0 until 2) {
        intercept[Exception] {
          Await.result(service(123))
        }
      }

      for (i <- 0 until markDeadForList.length) {
        // After another failure, the service should be unavailable
        intercept[Exception] {
          Await.result(service(123))
        }

        // Make sure the backoff follows the pattern above; after another
        // markDeadForList(i) - 1 seconds it should still be unavailable
        timeControl.advance(markDeadForList(i) - 1.second)
        timer.tick()

        // Now advance to + markDeadForList(i) seconds past marking dead, to equal the
        // backoff time
        timeControl.advance(1.second)
        timer.tick()
      }

      // Now succeed; markdead should be reset
      Await.result(service(456))

      // Fail again
      for (i <- 0 until 3) {
        when(underlyingService(123)) thenReturn Future.exception(new Exception)
        intercept[Exception] {
          Await.result(service(123))
        }
      }

      assert(!factory.isAvailable)
      assert(!service.isAvailable)

      timeControl.advance(markDeadForList(0))
      timer.tick()

      assert(factory.isAvailable)
      assert(service.isAvailable)
    }
  }

  test("a failing factory should be busy; done when revived") {
    Time.withCurrentTimeFrozen { tc =>
      val h = new Helper(consecutiveFailures)
      import h._

      assert(factory.status == Status.Open)
      intercept[Exception] {
        Await.result(service(123))
      }
      intercept[Exception] {
        Await.result(service(123))
      }
      assert(factory.status == Status.Open)
      intercept[Exception] {
        Await.result(service(123))
      }

      assert(factory.status == Status.Busy)

      tc.advance(10.seconds)
      timer.tick()

      assert(factory.status == Status.Open)
    }
  }

  test("a failing service should only be able to accept one request after " +
   "being revived, then multiple requests after it successfully completes") {
    val h = new Helper(consecutiveFailures)
    import h._

    Time.withCurrentTimeFrozen { tc =>
      for (i <- 1 to 3) {
        intercept[Exception] {
          Await.result(service(123))
        }
      }

      assert(factory.status == Status.Busy)

      tc.advance(10.seconds)
      timer.tick()

      assert(factory.status == Status.Open)

      when(underlyingService(456)).thenAnswer {
        new Answer[Future[Int]] {
          override def answer(invocation: InvocationOnMock) = {
            // The service should be busy after one request while probing
            assert(factory.status == Status.Busy)
            Future(456)
          }
        }
      }

      Await.result(service(456))
      assert(factory.status == Status.Open)
    }
  }

  test("a failing service should go back to the Busy state after probing fails") {
    val h = new Helper(consecutiveFailures)
    import h._

    Time.withCurrentTimeFrozen { tc =>
      for (i <- 1 to 3) {
        intercept[Exception] {
          Await.result(service(123))
        }
      }

      assert(factory.status == Status.Busy)

      tc.advance(10.seconds)
      timer.tick()

      assert(factory.status == Status.Open)

      when(underlyingService(456)).thenAnswer {
        new Answer[Future[Int]] {
          override def answer(invocation: InvocationOnMock) = {
            // The service should be busy after one request while probing
            assert(factory.status == Status.Busy)
            // Fail the probing request
            Future.exception(new Exception)
          }
        }
      }

      intercept[Exception] {
        Await.result(service(456))
      }

      // Should be busy after probe fails
      assert(factory.status == Status.Busy)
    }
  }

  test("a failing service should reset failure counters after an individual success") {
    val h = new Helper(consecutiveFailures)
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
      assert(statsReceiver.counters.get(List("removals")) == Some(1))
      assert(!factory.isAvailable)
      assert(!service.isAvailable)

      timeControl.advance(10.seconds)
      timer.tick()

      // Probing, not revived yet.
      assert(statsReceiver.counters.get(List("revivals")) == None)
      assert(statsReceiver.counters.get(List("removals")) == Some(1))
      assert(factory.isAvailable)
      assert(service.isAvailable)

      when(underlyingService(123)) thenReturn Future.value(321)
      Await.result(service(123))

      // A good dispatch; revived
      assert(statsReceiver.counters.get(List("revivals")) == Some(1))
      assert(statsReceiver.counters.get(List("removals")) == Some(1))
      assert(Await.result(service(123)) == 321)

      assert(factory.isAvailable)
      assert(service.isAvailable)

      // Counts are now reset.
      when(underlyingService(123)) thenReturn Future.exception(new Exception)
      intercept[Exception] {
        Await.result(service(123))
      }
      assert(statsReceiver.counters.get(List("revivals")) == Some(1))
      assert(statsReceiver.counters.get(List("removals")) == Some(1))
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
      assert(statsReceiver.counters.get(List("revivals")) == Some(1))
      assert(statsReceiver.counters.get(List("removals")) == Some(2))
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
      underlying, FailureAccrualPolicy.consecutiveFailures(3, FailureAccrualFactory.jitteredBackoff), new MockTimer, statsReceiver, "test")
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

    assert(service.status == Status.Busy)
  }

  class BrokenFactoryHelper {
    val statsReceiver = new InMemoryStatsReceiver()
    val underlying = mock[ServiceFactory[Int, Int]]
    when(underlying.close(any[Time])) thenReturn Future.Done
    when(underlying.status) thenReturn Status.Open
    val exc = new Exception("i broked :-(")
    when(underlying()) thenReturn Future.exception(exc)
    val factory = new FailureAccrualFactory[Int, Int](
      underlying, FailureAccrualPolicy.consecutiveFailures(3, FailureAccrualFactory.jitteredBackoff), new MockTimer, statsReceiver, "test")
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
      failureAccrualPolicy: FailureAccrualPolicy,
      timer: Timer,
      label: String
    ) extends FailureAccrualFactory[Int, Int](underlying, failureAccrualPolicy, timer, NullStatsReceiver, label) {
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
      underlying, FailureAccrualPolicy.consecutiveFailures(3, Backoff.const(5.seconds)), timer, "test")
    val service = Await.result(factory())
    verify(underlying)()
  }

  test("a customized factory should become unavailable") {
    val h = new CustomizedFactory
    import h._

    Time.withCurrentTimeFrozen { timeControl =>
      assert(Await.result(service(123)) == 321)
      assert(Await.result(service(123)) == 321)
      assert(factory.isAvailable)
      assert(service.isAvailable)

      // Now fail:
      assert(Await.result(service(123)) == 321)
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

    val failureAccrualPolicy = FailureAccrualPolicy.consecutiveFailures(42, Backoff.const(10.seconds))

    val p1: Param = Param.Configured(() => failureAccrualPolicy)
    val p2: Param = Replaced(_ => ServiceFactoryWrapper.identity)
    val p3: Param = Disabled

    assert((p1 match { case Param.Configured(x) => x() }) == failureAccrualPolicy)
    assert((p2 match { case Param.Replaced(f) => f(null) }) == ServiceFactoryWrapper.identity)
    assert(p3 match { case Disabled => true })

    val ps1: Stack.Params = Stack.Params.empty + p1
    assert(ps1.contains[Param])
    assert((ps1[Param] match { case Param.Configured(x) => x() }) == failureAccrualPolicy)

    val ps2: Stack.Params = Stack.Params.empty + p2 + p1
    assert(ps2.contains[Param])
    assert((ps2[Param] match { case Param.Configured(x) => x() }) == failureAccrualPolicy)

    val ps3: Stack.Params = Stack.Params.empty + p1 + p2 + p3
    assert(ps3.contains[Param])
    assert(ps3[Param] match { case Disabled => true })
  }

  test("module") {
    val h = new Helper(consecutiveFailures)
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
