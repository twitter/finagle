package com.twitter.finagle.liveness

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Backoff.EqualJittered
import com.twitter.finagle.service._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.util.Rng
import com.twitter.finagle.Backoff
import com.twitter.finagle._
import com.twitter.util._
import java.util.concurrent.TimeUnit
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.when
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatestplus.mockito.MockitoSugar
import scala.util.Random
import org.scalatest.funsuite.AnyFunSuite

class FailureAccrualFactoryTest extends AnyFunSuite with MockitoSugar {
  // since `EqualJittered` generates values randomly, we pass the seed
  // here in order to validate the values returned in the tests.
  def markDeadFor(seed: Long): Backoff =
    new EqualJittered(5.seconds, 5.seconds, 60.seconds, 1, Rng(seed))
  def markDeadForList(seed: Long) = markDeadFor(seed).take(6)
  def consecutiveFailures(seed: Long): FailureAccrualPolicy =
    FailureAccrualPolicy.consecutiveFailures(3, markDeadFor(seed))

  class ExceptionWithFailureFlags(val flags: Long = FailureFlags.Empty)
      extends FailureFlags[ExceptionWithFailureFlags] {

    def copyWithFlags(newFlags: Long): ExceptionWithFailureFlags =
      new ExceptionWithFailureFlags(newFlags)
  }

  val ignorableFailures = Seq(
    Failure.ignorable("ignore me!"),
    new ExceptionWithFailureFlags(FailureFlags.Ignorable)
  )

  class Helper(failureAccrualPolicy: FailureAccrualPolicy) {
    val statsReceiver = new InMemoryStatsReceiver
    val underlyingService = mock[Service[Int, Int]]
    when(underlyingService.close(any[Time])) thenReturn Future.Done
    when(underlyingService.status) thenReturn Status.Open
    when(underlyingService(ArgumentMatchers.anyInt)) thenReturn Future.exception(new Exception)

    val underlying = mock[ServiceFactory[Int, Int]]
    when(underlying.close(any[Time])) thenReturn Future.Done
    when(underlying.status) thenReturn Status.Open
    when(underlying()) thenReturn Future.value(underlyingService)

    val timer = new MockTimer

    val factory = new FailureAccrualFactory[Int, Int](
      underlying,
      failureAccrualPolicy,
      ResponseClassifier.Default,
      timer,
      statsReceiver
    )
    val service = Await.result(factory(), 5.seconds)
    verify(underlying)()
  }

  test("default policy is hybrid") {
    val faf = FailureAccrualFactory.defaultPolicy.toString
    assert(
      faf.contains("FailureAccrualPolicy.successRateWithinDuration") &&
        faf.contains("FailureAccrualPolicy.consecutiveFailures")
    )
  }

  test("a failing service should become unavailable") {
    val h = new Helper(consecutiveFailures(6666))
    import h._

    Time.withCurrentTimeFrozen { timeControl =>
      for (i <- 0 until 3) {
        assert(factory.isAvailable)
        assert(service.isAvailable)
        intercept[Exception] {
          Await.result(service(123), 5.seconds)
        }
      }
      // Now failed

      assert(statsReceiver.counters(List("removals")) == 1)
      assert(!factory.isAvailable)
      assert(!service.isAvailable)

      verify(underlyingService, times(3))(123)
    }
  }

  test("uses ResponseClassifier for determining success/failure/ignorable") {
    val svcFactory = ServiceFactory.const(Service.mk[Failure, Unit](Future.exception(_)))
    val classifier: ResponseClassifier = {
      case ReqRep(t: Failure, Throw(_)) if t.getMessage == "success" => ResponseClass.Success
      case ReqRep(_, Throw(t)) if t.getMessage == "ignore" => ResponseClass.Ignored
      case ReqRep(_, Throw(ex)) if ex.getMessage == "also success" => ResponseClass.Success
      case _ => ResponseClass.NonRetryableFailure
    }

    val stats = new InMemoryStatsReceiver()
    val faf = new FailureAccrualFactory[Failure, Unit](
      underlying = svcFactory,
      policy = FailureAccrualPolicy.consecutiveFailures(1, Backoff.const(2.seconds)),
      timer = Timer.Nil,
      statsReceiver = stats,
      responseClassifier = classifier
    )
    val svc = Await.result(faf(), 5.seconds)

    // normally these are failures, but these will not trip it...
    svc(Failure("success"))
    svc(Failure("also success"))
    assert(stats.counter("removals")() == 0)

    // now some ignorable exceptions
    svc(Failure("ignore"))
    assert(stats.counter("removals")() == 0)

    // trip it.
    svc(Failure("boom"))
    assert(stats.counter("removals")() == 1)
  }

  ignorableFailures.foreach { ignorableFailure =>
    test(s"does not count ignorable failure ($ignorableFailure) as failure") {
      val svcFactory = ServiceFactory.const {
        Service.const(Future.exception[Unit](ignorableFailure))
      }
      val stats = new InMemoryStatsReceiver()
      val faf = new FailureAccrualFactory[Unit, Unit](
        underlying = svcFactory,
        policy = FailureAccrualPolicy.consecutiveFailures(1, Backoff.const(2.seconds)),
        timer = Timer.Nil,
        statsReceiver = stats,
        responseClassifier = ResponseClassifier.Default
      )

      val svc = Await.result(faf(), 5.seconds)
      svc(())
      assert(stats.counter("removals")() == 0)
      assert(faf.isAvailable)
    }

    test(s"does not count ignorable failure ($ignorableFailure) as success") {
      var ret = Future.exception[Unit](new Exception("boom!"))
      val svcFactory = ServiceFactory.const {
        Service.mk { _: Unit => ret }
      }
      val stats = new InMemoryStatsReceiver()
      val faf = new FailureAccrualFactory[Unit, Unit](
        underlying = svcFactory,
        policy = FailureAccrualPolicy.consecutiveFailures(3, Backoff.const(2.seconds)),
        timer = new MockTimer,
        statsReceiver = stats,
        responseClassifier = ResponseClassifier.Default
      )

      val svc = Await.result(faf(), 5.seconds)
      svc(())
      svc(())

      assert(stats.counter("removals")() == 0)
      assert(faf.isAvailable)

      ret = Future.exception[Unit](ignorableFailure)

      svc(()) // this should not be counted as a success
      assert(stats.counter("removals")() == 0)
      assert(faf.isAvailable)

      ret = Future.exception[Unit](new Exception("boom!"))

      svc(()) // Third "real" exception in a row; should trip FA

      assert(stats.counter("removals")() == 1)
      assert(!faf.isAvailable)
    }

    test(s"keeps probe open on ignorable failure ($ignorableFailure)") {
      var ret = Future.exception[Unit](new Exception("boom!"))
      Time.withCurrentTimeFrozen { timeControl =>
        val svcFactory = ServiceFactory.const {
          Service.mk { _: Unit => ret }
        }
        val stats = new InMemoryStatsReceiver()
        val timer = new MockTimer
        val faf = new FailureAccrualFactory[Unit, Unit](
          underlying = svcFactory,
          policy = FailureAccrualPolicy.consecutiveFailures(1, Backoff.const(2.seconds)),
          timer = timer,
          statsReceiver = stats,
          responseClassifier = ResponseClassifier.Default
        )

        val svc = Await.result(faf(), 5.seconds)
        svc(())

        // Trip FA
        assert(stats.counter("removals")() == 1)
        assert(!faf.isAvailable)

        timeControl.advance(10.seconds)
        timer.tick()

        assert(faf.isAvailable)

        ret = Future.exception[Unit](ignorableFailure)

        svc(())

        // ensure that the ignorable not counted as a success, but that we can still send requests
        // (ProbeOpen state)
        assert(stats.counters(List("revivals")) == 0)
        assert(stats.counter("probes")() == 1)
        assert(stats.counter("removals")() == 1)
        assert(faf.isAvailable)
      }
    }
  }

  test("a failing service should enter the probing state after the markDeadFor duration") {
    val h = new Helper(consecutiveFailures(7777))
    import h._

    Time.withCurrentTimeFrozen { timeControl =>
      for (i <- 0 until 3) {
        intercept[Exception] {
          Await.result(service(123), 5.seconds)
        }
      }
      assert(statsReceiver.counters(List("removals")) == 1)
      assert(!factory.isAvailable)
      assert(!service.isAvailable)

      timeControl.advance(10.seconds)
      timer.tick()

      // Probing, not revived yet.
      assert(statsReceiver.counters(List("removals")) == 1)
      assert(statsReceiver.counters(List("revivals")) == 0)

      assert(factory.isAvailable)
      assert(service.isAvailable)

      // But after one bad dispatch, mark it again unhealthy.
      intercept[Exception] {
        Await.result(service(123), 5.seconds)
      }
      assert(statsReceiver.counters.get(List("probes")) == Some(1))
      assert(statsReceiver.counters.get(List("removals")) == Some(1))
      assert(!factory.isAvailable)
      assert(!service.isAvailable)
    }
  }

  test("a failing service should be revived on a backoff mechanism by default") {
    val h = new Helper(consecutiveFailures(8888))
    import h._

    Time.withCurrentTimeFrozen { timeControl =>
      when(underlyingService(456)) thenReturn Future.value(654)

      // 3 failures must occur before the service is initially removed,
      // then one failure after each re-instating
      intercept[Exception] {
        Await.result(service(123), 5.seconds)
      }
      intercept[Exception] {
        Await.result(service(123), 5.seconds)
      }
      // After another failure, the service should be unavailable
      intercept[Exception] {
        Await.result(service(123), 5.seconds)
      }

      // Backoff to verify against from the backoff passed to create a FailureAccrual policy
      // Should make sure to use the same seed
      var backoffs = new EqualJittered(5.seconds, 5.seconds, 60.seconds, 1, Rng(8888)).take(6)
      while (!backoffs.isExhausted) {
        assert(statsReceiver.counters.get(List("removals")) == Some(1))
        assert(!factory.isAvailable)
        assert(!service.isAvailable)

        // Make sure the backoff follows the pattern above; after another
        // backoffs.duration - 1 seconds it should still be unavailable
        timeControl.advance(backoffs.duration - 1.second)
        timer.tick()

        assert(statsReceiver.counters.get(List("removals")) == Some(1))
        assert(!factory.isAvailable)
        assert(!service.isAvailable)

        // Now advance to + backoffs.duration seconds past marking dead, to equal the
        // backoff time
        timeControl.advance(1.second)
        timer.tick()

        // The service should be available for a probe
        assert(statsReceiver.counters.get(List("removals")) == Some(1))
        assert(statsReceiver.counters(List("revivals")) == 0)
        assert(factory.isAvailable)
        assert(service.isAvailable)

        // After another failure, the service should be unavailable
        intercept[Exception] {
          Await.result(service(123), 5.seconds)
        }
        val probeStat = statsReceiver.counters.get(List("probes"))
        assert(probeStat.isDefined && probeStat.get >= 1)
        backoffs = backoffs.next
      }
    }
  }

  test("backoff should be 5 minutes when stream runs out") {
    // Backoff to pass to create a FailureAccrual policy
    val markDeadForFA = new EqualJittered(5.seconds, 5.seconds, 60.seconds, 1, Rng(7777)).take(3)
    // Backoff to verify, should use the same seed as the policy passed to FA
    var markDeadFor = new EqualJittered(5.seconds, 5.seconds, 60.seconds, 1, Rng(7777)).take(3)

    val statsReceiver = new InMemoryStatsReceiver()
    val underlyingService = mock[Service[Int, Int]]
    when(underlyingService.close(any[Time])) thenReturn Future.Done
    when(underlyingService.status) thenReturn Status.Open
    when(underlyingService(ArgumentMatchers.anyInt)) thenReturn Future.exception(new Exception)

    val underlying = mock[ServiceFactory[Int, Int]]
    when(underlying.close(any[Time])) thenReturn Future.Done
    when(underlying.status) thenReturn Status.Open
    when(underlying()) thenReturn Future.value(underlyingService)

    val timer = new MockTimer

    val factory = new FailureAccrualFactory[Int, Int](
      underlying = underlying,
      policy = FailureAccrualPolicy.consecutiveFailures(3, markDeadForFA),
      responseClassifier = ResponseClassifier.Default,
      timer = timer,
      statsReceiver = statsReceiver
    )
    val service = Await.result(factory())
    verify(underlying)()

    Time.withCurrentTimeFrozen { timeControl =>
      // 3 failures must occur before the service is initially removed,
      // then one failure after each re-instating
      intercept[Exception] {
        Await.result(service(123), 5.seconds)
      }
      intercept[Exception] {
        Await.result(service(123), 5.seconds)
      }

      while (!markDeadFor.isExhausted) {
        // After another failure, the service should be unavailable
        intercept[Exception] {
          Await.result(service(123), 5.seconds)
        }

        assert(statsReceiver.counters.get(List("removals")) == Some(1))
        assert(!factory.isAvailable)
        assert(!service.isAvailable)

        // Make sure the backoff follows the pattern above; after another
        // markDeadFor.duration - 1 seconds it should still be unavailable
        timeControl.advance(markDeadFor.duration - 1.second)
        timer.tick()

        assert(statsReceiver.counters.get(List("removals")) == Some(1))
        assert(!factory.isAvailable)
        assert(!service.isAvailable)

        // Now advance to + markDeadFor.duration seconds past marking dead, to equal the
        // backoff time
        timeControl.advance(1.second)
        timer.tick()

        // The service should be available for a probe
        assert(statsReceiver.counters.get(List("removals")) == Some(1))
        assert(statsReceiver.counters(List("revivals")) == 0)
        assert(factory.isAvailable)
        assert(service.isAvailable)

        markDeadFor = markDeadFor.next
      }

      intercept[Exception] {
        Await.result(service(123), 5.seconds)
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
    val h = new Helper(consecutiveFailures(9999))
    import h._

    Time.withCurrentTimeFrozen { timeControl =>
      when(underlyingService(456)) thenReturn Future.value(654)

      // 3 failures must occur before the service is initially removed,
      // then one failure after each probing
      for (i <- 0 until 2) {
        intercept[Exception] {
          Await.result(service(123), 5.seconds)
        }
      }

      // Backoff to verify against from the backoff passed to create a FailureAccrual policy
      // Should make sure to use the same seed
      var markDeadFor = new EqualJittered(5.seconds, 5.seconds, 60.seconds, 1, Rng(9999)).take(6)
      for (_ <- 1 until 6) {
        // After another failure, the service should be unavailable
        intercept[Exception] {
          Await.result(service(123), 5.seconds)
        }

        // Make sure the backoff follows the pattern above; after another
        // markDeadFor.duration - 1 seconds it should still be unavailable
        timeControl.advance(markDeadFor.duration - 1.second)
        timer.tick()

        // Now advance to + markDeadFor.duration seconds past marking dead, to equal the
        // backoff time
        timeControl.advance(1.second)
        timer.tick()

        markDeadFor = markDeadFor.next
      }

      // Now succeed; markDead should be reset
      Await.result(service(456), 5.seconds)

      // Fail again
      for (i <- 0 until 3) {
        when(underlyingService(123)) thenReturn Future.exception(new Exception)
        intercept[Exception] {
          Await.result(service(123), 5.seconds)
        }
      }

      assert(!factory.isAvailable)
      assert(!service.isAvailable)

      timeControl.advance(markDeadFor.duration)
      timer.tick()

      assert(factory.isAvailable)
      assert(service.isAvailable)
    }
  }

  test("a failing factory should be busy; done when revived") {
    Time.withCurrentTimeFrozen { tc =>
      val h = new Helper(consecutiveFailures(6666))
      import h._

      assert(factory.status == Status.Open)
      intercept[Exception] {
        Await.result(service(123), 5.seconds)
      }
      intercept[Exception] {
        Await.result(service(123))
      }
      assert(factory.status == Status.Open)
      intercept[Exception] {
        Await.result(service(123), 5.seconds)
      }

      assert(factory.status == Status.Busy)

      tc.advance(10.seconds)
      timer.tick()

      assert(factory.status == Status.Open)
    }
  }

  test(
    "a failing service should only be able to accept one request after " +
      "being revived, then multiple requests after it successfully completes"
  ) {
    val h = new Helper(consecutiveFailures(7777))
    import h._

    Time.withCurrentTimeFrozen { tc =>
      for (i <- 1 to 3) {
        intercept[Exception] {
          Await.result(service(123), 5.seconds)
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

      Await.result(service(456), 5.seconds)
      assert(factory.status == Status.Open)
    }
  }

  test("a failing service should go back to the Busy state after probing fails") {
    val h = new Helper(consecutiveFailures(9999))
    import h._

    Time.withCurrentTimeFrozen { tc =>
      for (i <- 1 to 3) {
        intercept[Exception] {
          Await.result(service(123), 5.seconds)
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
        Await.result(service(456), 5.seconds)
      }

      // Should be busy after probe fails
      assert(factory.status == Status.Busy)
    }
  }

  test("a failing service should reset failure counters after an individual success") {
    val h = new Helper(consecutiveFailures(3333))
    import h._

    Time.withCurrentTimeFrozen { timeControl =>
      intercept[Exception] {
        Await.result(service(123), 5.seconds)
      }
      intercept[Exception] {
        Await.result(service(123), 5.seconds)
      }
      intercept[Exception] {
        Await.result(service(123), 5.seconds)
      }
      assert(statsReceiver.counters.get(List("removals")) == Some(1))
      assert(!factory.isAvailable)
      assert(!service.isAvailable)

      timeControl.advance(10.seconds)
      timer.tick()

      // Probing, not revived yet.
      assert(statsReceiver.counters(List("revivals")) == 0)
      assert(statsReceiver.counters.get(List("removals")) == Some(1))
      assert(factory.isAvailable)
      assert(service.isAvailable)

      when(underlyingService(123)) thenReturn Future.value(321)
      Await.result(service(123), 5.seconds)

      // A good dispatch; revived
      assert(statsReceiver.counters.get(List("revivals")) == Some(1))
      assert(statsReceiver.counters.get(List("removals")) == Some(1))
      assert(Await.result(service(123), 5.seconds) == 321)

      assert(factory.isAvailable)
      assert(service.isAvailable)

      // Counts are now reset.
      when(underlyingService(123)) thenReturn Future.exception(new Exception)
      intercept[Exception] {
        Await.result(service(123), 5.seconds)
      }
      assert(statsReceiver.counters.get(List("revivals")) == Some(1))
      assert(statsReceiver.counters.get(List("removals")) == Some(1))
      assert(factory.isAvailable)
      assert(service.isAvailable)
      intercept[Exception] {
        Await.result(service(123), 5.seconds)
      }
      assert(factory.isAvailable)
      assert(service.isAvailable)
      intercept[Exception] {
        Await.result(service(123), 5.seconds)
      }
      assert(statsReceiver.counters.get(List("revivals")) == Some(1))
      assert(statsReceiver.counters.get(List("removals")) == Some(2))
      assert(!factory.isAvailable)
      assert(!service.isAvailable)
    }
  }

  test("A failure during probing that does not mark dead moves back to probing") {
    val policy = new FailureAccrualPolicy {
      val name = "Foo"
      def show() = name

      var markDead = true

      def recordSuccess() = ()
      def revived() = ()
      def markDeadOnFailure(): Option[Duration] = {
        if (markDead) {
          markDead = false
          Some(1.second)
        } else None
      }
    }

    val h = new Helper(policy)
    import h._

    Time.withCurrentTimeFrozen { tc =>
      // Fail a request to mark dead
      intercept[Exception] {
        Await.result(service(123), 5.seconds)
      }
      assert(!factory.isAvailable)
      assert(!service.isAvailable)

      // Advance past period
      tc.advance(2.seconds)
      timer.tick()

      // Although the underlying service is failing, the policy tells us
      // we are healthy. Make sure we accept more requests to reconcile
      // with the policy.
      intercept[Exception] {
        Await.result(service(123), 5.seconds)
      }
      assert(factory.isAvailable)
      assert(service.isAvailable)
    }
  }

  class HealthyServiceHelper {
    val statsReceiver = new InMemoryStatsReceiver()
    val underlyingService = mock[Service[Int, Int]]
    when(underlyingService.close(any[Time])) thenReturn Future.Done
    when(underlyingService.status) thenReturn Status.Open
    when(underlyingService(ArgumentMatchers.anyInt)) thenReturn Future.value(321)

    val underlying = mock[ServiceFactory[Int, Int]]
    when(underlying.close(any[Time])) thenReturn Future.Done
    when(underlying.status) thenReturn Status.Open
    when(underlying()) thenReturn Future.value(underlyingService)

    val factory = new FailureAccrualFactory[Int, Int](
      underlying = underlying,
      policy = FailureAccrualPolicy.consecutiveFailures(3, FailureAccrualFactory.jitteredBackoff),
      responseClassifier = ResponseClassifier.Default,
      timer = new MockTimer,
      statsReceiver = statsReceiver
    )
    val service = Await.result(factory(), 5.seconds)
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
    val timer = new MockTimer
    val factory = new FailureAccrualFactory[Int, Int](
      underlying,
      FailureAccrualPolicy.consecutiveFailures(3, FailureAccrualFactory.jitteredBackoff),
      ResponseClassifier.Default,
      timer,
      statsReceiver
    )
  }

  test("a broken factory should fail after the given number of tries") {
    val h = new BrokenFactoryHelper
    import h._

    Time.withCurrentTimeFrozen { timeControl =>
      for (i <- 1 to 3) {
        assert(factory.isAvailable)
        intercept[Exception] {
          Await.result(factory(), 5.seconds)
        }
      }
      assert(!factory.isAvailable)

      // Advance past period
      timeControl.advance(10.seconds)
      timer.tick()

      // Probing should fail due to factory exception. It should stop the probing and mark it dead again
      intercept[Exception] {
        Await.result(factory(), 5.seconds)
      }
      assert(statsReceiver.counters.get(List("probes")) == Some(1))
      assert(statsReceiver.counters.get(List("removals")) == Some(1))
      assert(!factory.isAvailable)
      assert(factory.status == Status.Busy)
    }
  }

  class CustomizedFactory {
    class CustomizedFailureAccrualFactory(
      underlying: ServiceFactory[Int, Int],
      failureAccrualPolicy: FailureAccrualPolicy,
      responseClassifier: ResponseClassifier,
      timer: Timer)
        extends FailureAccrualFactory[Int, Int](
          underlying,
          failureAccrualPolicy,
          responseClassifier,
          timer,
          NullStatsReceiver
        ) {
      override def classify(reqRep: ReqRep): ResponseClass = {
        reqRep.response match {
          case Return(x) if x != 321 => ResponseClass.Success
          case _ => ResponseClass.NonRetryableFailure
        }
      }
    }

    val underlyingService = mock[Service[Int, Int]]
    when(underlyingService.close(any[Time])) thenReturn Future.Done
    when(underlyingService.status) thenReturn Status.Open
    when(underlyingService(ArgumentMatchers.anyInt)) thenReturn Future.value(321)

    val underlying = mock[ServiceFactory[Int, Int]]
    when(underlying.close(any[Time])) thenReturn Future.Done
    when(underlying.status) thenReturn Status.Open
    when(underlying()) thenReturn Future.value(underlyingService)

    val timer = new MockTimer
    val factory = new CustomizedFailureAccrualFactory(
      underlying,
      FailureAccrualPolicy.consecutiveFailures(3, Backoff.const(5.seconds)),
      ResponseClassifier.Default,
      timer
    )
    val service = Await.result(factory(), 5.seconds)
    verify(underlying)()
  }

  test("a customized factory should become unavailable") {
    val h = new CustomizedFactory
    import h._

    Time.withCurrentTimeFrozen { timeControl =>
      assert(Await.result(service(123), 5.seconds) == 321)
      assert(Await.result(service(123), 5.seconds) == 321)
      assert(factory.isAvailable)
      assert(service.isAvailable)

      // Now fail:
      assert(Await.result(service(123), 5.seconds) == 321)
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

    val failureAccrualPolicy =
      FailureAccrualPolicy.consecutiveFailures(42, Backoff.const(10.seconds))

    val p1: Param = Param.Configured(() => failureAccrualPolicy)
    val p2: Param = Replaced(_ => ServiceFactoryWrapper.identity)
    val p3: Param = Disabled

    assert((p1 match {
      case Param.Configured(x) => x()
      case x => throw new MatchError(x)
    }) == failureAccrualPolicy)

    assert((p2 match {
      case Param.Replaced(f) => f(null)
      case x => throw new MatchError(x)
    }) == ServiceFactoryWrapper.identity)

    assert(p3 match {
      case Disabled => true
      case x => throw new MatchError(x)
    })

    val ps1: Stack.Params = Stack.Params.empty + p1
    assert(ps1.contains[Param])
    assert((ps1[Param] match {
      case Param.Configured(x) => x()
      case x => throw new MatchError(x)
    }) == failureAccrualPolicy)

    val ps2: Stack.Params = Stack.Params.empty + p2 + p1
    assert(ps2.contains[Param])
    assert((ps2[Param] match {
      case Param.Configured(x) => x()
      case x => throw new MatchError(x)
    }) == failureAccrualPolicy)

    val ps3: Stack.Params = Stack.Params.empty + p1 + p2 + p3
    assert(ps3.contains[Param])
    assert(ps3[Param] match {
      case Disabled => true
      case x => throw new MatchError(x)
    })
  }

  test("module") {
    val h = new Helper(consecutiveFailures(3333))
    val s: Stack[ServiceFactory[Int, Int]] =
      FailureAccrualFactory
        .module[Int, Int]
        .toStack(Stack.leaf(Stack.Role("Service"), h.underlying))

    val ps: Stack.Params = Stack.Params.empty + param.Stats(h.statsReceiver)

    // disabled
    Await.ready(s.make(ps + FailureAccrualFactory.Disabled).toService(10))
    assert(!h.statsReceiver.counters.contains(Seq("failure_accrual", "removals")))

    // replaced
    Await.ready(
      s.make(ps + FailureAccrualFactory.Replaced(ServiceFactoryWrapper.identity)).toService(10)
    )
    assert(!h.statsReceiver.counters.contains(Seq("failure_accrual", "removals")))

    // configured
    Await.ready(s.make(ps + FailureAccrualFactory.Param(1, Duration.Top)).toService(10))
    assert(h.statsReceiver.counters.contains(Seq("failure_accrual", "removals")))
  }
}
