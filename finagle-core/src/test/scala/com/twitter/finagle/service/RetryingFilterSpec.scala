package com.twitter.finagle.service

import RetryPolicy._
import com.twitter.conversions.time._
import com.twitter.finagle.stats.{Stat, StatsReceiver}
import com.twitter.finagle.{CancelledRequestException, MockTimer, Service, TimeoutException, WriteException}
import com.twitter.util._
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class RetryingFilterSpec extends SpecificationWithJUnit with Mockito {
  "RetryPolicy" should {
    import RetryPolicy._
    val NoExceptions: PartialFunction[Try[Nothing], Boolean] = {
      case _ => false
    }
    val timeoutExc = new TimeoutException {
      protected val timeout = 0.seconds
      protected val explanation = "!"
    }

    "WriteExceptionsOnly" in {
      val weo = WriteExceptionsOnly orElse NoExceptions

      weo(Throw(new Exception)) must beFalse
      weo(Throw(WriteException(new Exception))) must beTrue
      weo(Throw(WriteException(new CancelledRequestException))) must beFalse
      weo(Throw(timeoutExc)) must beFalse
    }

    "TimeoutAndWriteExceptionsOnly" in {
      val weo = TimeoutAndWriteExceptionsOnly orElse NoExceptions

      weo(Throw(new Exception)) must beFalse
      weo(Throw(WriteException(new Exception))) must beTrue
      weo(Throw(WriteException(new CancelledRequestException))) must beFalse
      weo(Throw(timeoutExc)) must beTrue
    }
  }

  "RetryingFilter" should {
    val backoffs = Stream(1.second, 2.seconds, 3.seconds)
    val stats = mock[StatsReceiver]
    val retriesStat = mock[Stat]
    val timer = new MockTimer
    stats.stat("retries") returns retriesStat
    val shouldRetry = mock[PartialFunction[Try[Nothing], Boolean]]
    shouldRetry.isDefinedAt(any) returns true
    shouldRetry(any[Try[Nothing]]) answers {
      case Throw(_: WriteException) => true
      case _ => false
    }

    "with RetryPolicy.tries" in {
      val filter = new RetryingFilter[Int, Int](RetryPolicy.tries(3, shouldRetry), timer, stats)
      val service = mock[Service[Int, Int]]
      service.close(any) returns Future.Done
      val retryingService = filter andThen service

      "always try once" in {
        service(123) returns Future(321)
        Await.result(retryingService(123)) must be_==(321)
        there was one(service)(123)
        there was one(retriesStat).add(0)
      }

      "when failing with WriteExceptions, retry n-1 times" in {
        service(123) returns Future.exception(WriteException(new Exception))
        val f = retryingService(123)
        there were three(service)(123)
        Await.result(f) must throwA[WriteException]
      }

      "when failed with a non-WriteException, fail immediately" in {
        service(123) returns Future.exception(new Exception("WTF!"))
        Await.result(retryingService(123)) must throwA(new Exception("WTF!"))
        there was one(service)(123)
        there was one(retriesStat).add(0)
      }

      "when no retry occurs, no stat update" in {
        service(123) returns Future(321)
        Await.result(retryingService(123)) must be_==(321)
        there was one(retriesStat).add(0)
      }

      "propagate interrupts" in {
        val replyPromise = new Promise[Int] {
          @volatile var interrupted: Option[Throwable] = None
          setInterruptHandler { case exc => interrupted = Some(exc) }
        }
        service(123) returns replyPromise

        val res = retryingService(123)
        res.isDefined must beFalse
        replyPromise.interrupted must beNone

        val exc = new Exception
        res.raise(exc)
        res.isDefined must beFalse
        replyPromise.interrupted must beSome(exc)
      }
    }

    "with RetryPolicy.backoff" in
      testPolicy(RetryPolicy.backoff(backoffs)(shouldRetry))
    "with RetryPolicy.backoffJava" in
      testPolicy(RetryPolicy.backoffJava(Backoff.toJava(backoffs), shouldRetry))

    def testPolicy(policy: RetryPolicy[Try[Nothing]]) {
      val filter = new RetryingFilter[Int, Int](policy, timer, stats)
      val service = mock[Service[Int, Int]]
      service.close(any) returns Future.Done
      val retryingService = filter andThen service

      "always try once" in {
        service(123) returns Future(321)
        Await.result(retryingService(123)) must be_==(321)
        there was one(service)(123)
        there was one(retriesStat).add(0)
      }

      "when failed with a WriteException, consult the retry strategy" in Time.withCurrentTimeFrozen { tc =>
        service(123) returns Future.exception(WriteException(new Exception))
        val f = retryingService(123)
        there was one(service)(123)
        f.isDefined must beFalse
        timer.tasks must haveSize(1)

        service(123) returns Future(321)  // we succeed next time; tick!
        tc.advance(1.second); timer.tick()

        there were two(service)(123)
        there was one(retriesStat).add(1)
        Await.result(f) must be_==(321)
      }

      "give up when the retry strategy is exhausted" in Time.withCurrentTimeFrozen { tc =>
        service(123) returns Future.exception(WriteException(new Exception("i'm exhausted")))
        val f = retryingService(123)
        1 to 3 foreach { i =>
          f.isDefined must beFalse
          there were i.times(service)(123)
          there was no(retriesStat).add(3)
          tc.advance(i.seconds); timer.tick()
        }

        there was one(retriesStat).add(3)
        f.isDefined must beTrue
        Await.ready(f).poll.get.isThrow must beTrue
        Await.result(f) must throwA(WriteException(new Exception("i'm exhausted")))
      }

      "when failed with a non-WriteException, fail immediately" in {
        service(123) returns Future.exception(new Exception("WTF!"))
        Await.result(retryingService(123)) must throwA(new Exception("WTF!"))
        there was one(service)(123)
        timer.tasks must beEmpty
        there was one(retriesStat).add(0)
      }

      "when no retry occurs, no stat update" in {
        service(123) returns Future(321)
        Await.result(retryingService(123)) must be_==(321)
        there was one(retriesStat).add(0)
      }

      "propagate cancellation" in {
        val replyPromise = new Promise[Int] {
          @volatile var interrupted: Option[Throwable] = None
          setInterruptHandler { case exc => interrupted = Some(exc) }
        }
        service(123) returns replyPromise

        val res = retryingService(123)
        res.isDefined must beFalse
        replyPromise.interrupted must beNone

        val exc = new Exception
        res.raise(exc)
        res.isDefined must beFalse
        replyPromise.interrupted must beSome(exc)
      }
    }
  }

  "Backoff" should {
    "Backoff.exponential" in {
      val backoffs = Backoff.exponential(1.seconds, 2) take 10
      backoffs.force.toSeq must be_==(0 until 10 map { i => (1 << i).seconds })
    }

    "Backoff.exponential with upper limit" in {
      val backoffs = (Backoff.exponential(1.seconds, 2) take 5) ++ Backoff.const(32.seconds)
      (backoffs take 10).force.toSeq must be_==(0 until 10 map {
          i => (math.min(1 << i, 32)).seconds })
    }

    "Backoff.linear" in {
      val backoffs = Backoff.linear(2.seconds, 10.seconds) take 10
      backoffs.head must be_==(2.seconds)
      backoffs.tail.force.toSeq must be_==(1 until 10 map { i => 2.seconds + 10.seconds * i })
    }

    "Backoff.const" in {
      val backoffs = Backoff.const(10.seconds) take 10
      backoffs.force.toSeq must be_==(0 until 10 map { _ => 10.seconds})
    }
  }
}
