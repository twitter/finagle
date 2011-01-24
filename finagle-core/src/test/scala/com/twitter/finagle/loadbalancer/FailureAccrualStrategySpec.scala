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
    val service1 = mock[Service[Int, Int]]
    val services = Seq(service, service1)
    val underlying = mock[LoadBalancerStrategy[Int, Int]]
    val servicesCaptor = ArgumentCaptor.forClass(classOf[Seq[Service[Int, Int]]])

    underlying.dispatch(123, services) returns Some(service, Future.exception(new Exception))
    underlying.dispatch(123, Seq(service1)) returns Some(service, Future.value(321))
    underlying.dispatch(123, Seq()) returns None

    "filter out services that are dead" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val failureAccrual =
          new FailureAccrualStrategy[Int, Int](underlying, 3, 10.seconds)

        failureAccrual.dispatch(123, services) must beSomething
        there was one(underlying).dispatch(123, services)

        failureAccrual.dispatch(123, services) must beSomething
        there were two(underlying).dispatch(123, services)

        failureAccrual.dispatch(123, services) must beSomething
        there were three(underlying).dispatch(123, services)

        // ``service'' should now be dead. limit to ``service1''.
        failureAccrual.dispatch(123, services) must beSomething
        there were three(underlying).dispatch(123, services)
        there were one(underlying).dispatch(123, Seq(service1))
      }
    }

    "revive a service (for one request) after the markDeadFor duration" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val failureAccrual =
          new FailureAccrualStrategy[Int, Int](underlying, 2, 10.seconds)

        failureAccrual.dispatch(123, services) must beSomething
        there was one(underlying).dispatch(123, services)

        failureAccrual.dispatch(123, services) must beSomething
        there were two(underlying).dispatch(123, services)
        there was no(underlying).dispatch(123, Seq(service1))

        // Unhealthy now.
        failureAccrual.dispatch(123, services) must beSomething
        there were two(underlying).dispatch(123, services)
        there was one(underlying).dispatch(123, Seq(service1))

        timeControl.advance(10.seconds)
         
        // Healthy again.
        failureAccrual.dispatch(123, services) must beSomething
        there were three(underlying).dispatch(123, services)
        there was one(underlying).dispatch(123, Seq(service1))
         
        // Unhealthy again.
        failureAccrual.dispatch(123, services) must beSomething
        there were 4.times(underlying).dispatch(123, services)
        there were one(underlying).dispatch(123, Seq(service1))
         
        // Marked dead.
        failureAccrual.dispatch(123, services) must beSomething
        there were 4.times(underlying).dispatch(123, services)
        there were two(underlying).dispatch(123, Seq(service1))
      }
    }

    "reset failure counters after an individual success" in {
      Time.withCurrentTimeFrozen { timeControl =>
        val failureAccrual =
          new FailureAccrualStrategy[Int, Int](underlying, 2, 10.seconds)

        failureAccrual.dispatch(123, services) must beSomething
        there was one(underlying).dispatch(123, services)

        // One success. This should reset the counter.
        underlying.dispatch(123, services) returns Some(service, Future(321))
        failureAccrual.dispatch(123, services) must beSomething
        there were two(underlying).dispatch(123, services)

        // Make it fail again.
        (underlying.dispatch(123, services)
         returns Some(service, Future.exception(new Exception)))

        failureAccrual.dispatch(123, services) must beSomething
        there were three(underlying).dispatch(123, services)

        failureAccrual.dispatch(123, services) must beSomething
        there were 4.times(underlying).dispatch(123, services)

        failureAccrual.dispatch(123, services) must beSomething
        there were 4.times(underlying).dispatch(123, services)
        there was one(underlying).dispatch(123, Seq(service1))
      }      
    }
  }
}
