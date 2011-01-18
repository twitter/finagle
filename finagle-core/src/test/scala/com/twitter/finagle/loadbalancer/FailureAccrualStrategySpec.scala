package com.twitter.finagle.loadbalancer

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.{Matchers, ArgumentCaptor}

import com.twitter.util.{Time, Future}
import com.twitter.conversions.time._

import com.twitter.finagle.Service

object FailureAccrualStrategySpec extends Specification with Mockito {
  "FailureAccrualStrategy" should {
    val service = mock[Service[Int, Int]]
    val underlying = mock[LoadBalancerStrategy[Int, Int]]
    val servicesCaptor = ArgumentCaptor.forClass(classOf[Seq[Service[Int, Int]]])
    (underlying.dispatch(123, Seq(service))
       returns Some(service, Future.exception(new Exception)))
    underlying.dispatch(123, Seq()) returns None

    "filter out services that are dead" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val failureAccrual =
          new FailureAccrualStrategy[Int, Int](underlying, 3, 10.seconds)

        failureAccrual.dispatch(123, Seq(service)) must beSomething
        there was one(underlying).dispatch(123, Seq(service))

        failureAccrual.dispatch(123, Seq(service)) must beSomething
        there were two(underlying).dispatch(123, Seq(service))

        failureAccrual.dispatch(123, Seq(service)) must beSomething
        there were three(underlying).dispatch(123, Seq(service))

        failureAccrual.dispatch(123, Seq(service)) must beNone
        there were three(underlying).dispatch(123, Seq(service))
      }
    }

    "revive a service (for one request) after the markDeadFor duration" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val failureAccrual =
          new FailureAccrualStrategy[Int, Int](underlying, 2, 10.seconds)

        failureAccrual.dispatch(123, Seq(service)) must beSomething
        there was one(underlying).dispatch(123, Seq(service))

        failureAccrual.dispatch(123, Seq(service)) must beSomething
        there were two(underlying).dispatch(123, Seq(service))

        failureAccrual.dispatch(123, Seq(service)) must beNone
        there were two(underlying).dispatch(123, Seq(service))

        timeControl.advance(10.seconds)

        // Healthy for a bit.
        failureAccrual.dispatch(123, Seq(service)) must beSomething
        there were three(underlying).dispatch(123, Seq(service))

        // Back to being unhealthy.
        failureAccrual.dispatch(123, Seq(service)) must beNone
        there were three(underlying).dispatch(123, Seq(service))
      }
    }

    "reset failure counters after an individual success" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val failureAccrual =
          new FailureAccrualStrategy[Int, Int](underlying, 2, 10.seconds)

        failureAccrual.dispatch(123, Seq(service)) must beSomething
        there was one(underlying).dispatch(123, Seq(service))

        // One success. This should reset the counter.
        underlying.dispatch(123, Seq(service)) returns Some(service, Future(321))
        failureAccrual.dispatch(123, Seq(service)) must beSomething
        there were two(underlying).dispatch(123, Seq(service))

        // Make it fail again.
        (underlying.dispatch(123, Seq(service))
         returns Some(service, Future.exception(new Exception)))

        failureAccrual.dispatch(123, Seq(service)) must beSomething
        there were three(underlying).dispatch(123, Seq(service))

        failureAccrual.dispatch(123, Seq(service)) must beSomething
        there were 4.times(underlying).dispatch(123, Seq(service))

        failureAccrual.dispatch(123, Seq(service)) must beNone
        there were 4.times(underlying).dispatch(123, Seq(service))
      }      
    }
  }
}
