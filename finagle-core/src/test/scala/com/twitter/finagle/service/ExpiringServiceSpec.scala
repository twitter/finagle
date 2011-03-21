package com.twitter.finagle.service

import org.specs.Specification
import org.specs.mock.Mockito

import com.twitter.finagle.{Service, WriteException}
import com.twitter.finagle.MockTimer

import com.twitter.util.{Time, Promise, Return}
import com.twitter.conversions.time._

object ExpiringServiceSpec extends Specification with Mockito {
  "ExpiringService" should {
    Time.withCurrentTimeFrozen { timeControl =>
      val timer = new MockTimer
      val underlying = mock[Service[Any, Any]]
      val promise = new Promise[Int]
      underlying(123) returns promise
      underlying.isAvailable returns true


      "idle time between requests" in {
        val service = new ExpiringService[Any, Any](underlying, Some(10.seconds), None, timer)
        timer.tasks must haveSize(1)
        "expire after the given idle time" in {
          // For some reason, this complains of different types:
          //   timer.tasks.head.when must be_==(Time.now + 10.seconds)
          service.isAvailable must beTrue

          timeControl.advance(10.seconds)
          timer.tick()

          service.isAvailable must beFalse
          there was one(underlying).release()

          timer.tasks must beEmpty
        }

        "cancel the timer when a request is issued" in {
          service(123)
          timer.tasks must beEmpty
        }

        "restart the timer when the request finished" in {
          service(123)
          timer.tasks must beEmpty

          timeControl.advance(10.seconds)
          timer.tick()

          timer.tasks must beEmpty
          promise() = Return(321)
          timer.tasks must haveSize(1)

          there was no(underlying).release()
          timeControl.advance(10.seconds)
          timer.tick()

          there was one(underlying).release()
        }

        "throw an write exception if we attempt to use an expired service" in {
          timeControl.advance(10.seconds)
          timer.tick()

          service(132)() must throwA[WriteException]
        }
      }
      
      "life time of a connection" in {
        val service = new ExpiringService[Any, Any](underlying, None, Some(10.seconds), timer)
        timer.tasks must haveSize(1)
        "expire after the given idle time" in {
          // For some reason, this complains of different types:
          //   timer.tasks.head.when must be_==(Time.now + 10.seconds)
          service.isAvailable must beTrue

          timeControl.advance(10.seconds)
          timer.tick()

          service.isAvailable must beFalse
          there was one(underlying).release()

          timer.tasks must beEmpty
        }

        "does not cancel the timer when a request is issued" in {
          service(123)
          timer.tasks must haveSize(1)
          timer.tasks.head.isCancelled must beFalse
        }
        
        "throw an write exception if we attempt to use an expired service" in {
          timeControl.advance(10.seconds)
          timer.tick()

          service(132)() must throwA[WriteException]
        }
      }
      
      "idle timer fires before life timer fires" in {
        val service = new ExpiringService[Any, Any](underlying, Some(10.seconds), Some(1.minute), timer)
        timer.tasks must haveSize(2)
        
        "expire after the given idle time" in {
          // For some reason, this complains of different types:
          //   timer.tasks.head.when must be_==(Time.now + 10.seconds)
          service.isAvailable must beTrue

          timeControl.advance(10.seconds)
          timer.tick()

          service.isAvailable must beFalse
          there was one(underlying).release()

          timer.tasks must beEmpty
        }                
      }
      
      "life timer fires before idle timer fires" in {
        val service = new ExpiringService[Any, Any](underlying, Some(10.seconds), Some(15.seconds), timer)
        timer.tasks must haveSize(2)
        timer.tasks forall(!_.isCancelled) must beTrue
        
        "expire after the given life time" in {
          service(123)
          timer.tasks must haveSize(1)
          timer.tasks.head.isCancelled must beFalse
         
          timeControl.advance(8.seconds)
          timer.tick()

          timer.tasks must haveSize(1)
          timer.tasks.head.isCancelled must beFalse
          
          promise() = Return(321)
          timer.tasks must haveSize(2)
          timer.tasks forall(!_.isCancelled) must beTrue

          there was no(underlying).release()
          timeControl.advance(8.seconds)
          timer.tick()

          timer.tasks must beEmpty
          service.isAvailable must beFalse
          there was one(underlying).release()
        }
      }
      
      "life timer fires while there are requests" in {
        val service = new ExpiringService[Any, Any](underlying, Some(10.seconds), Some(5.seconds), timer)
        timer.tasks must haveSize(2)
        timer.tasks forall(!_.isCancelled) must beTrue
        
        "expire after the given life time" in {
          service(123)
          timer.tasks must haveSize(1)
          timer.tasks.head.isCancelled must beFalse
         
          timeControl.advance(8.seconds)
          timer.tick()

          timer.tasks must beEmpty
          service.isAvailable must beFalse
          there was no(underlying).release()
          
          promise() = Return(321)
          timer.tasks must beEmpty
          service.isAvailable must beFalse
          there was one(underlying).release()
          
          service(132)() must throwA[WriteException]
        }
      }
    }
  }
}
