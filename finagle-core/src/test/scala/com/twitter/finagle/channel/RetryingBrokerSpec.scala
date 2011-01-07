package com.twitter.finagle.channel

import org.specs.Specification
import org.specs.mock.Mockito
import org.jboss.netty.channel._

import com.twitter.util.Future

class RetryingBrokerSpec extends Specification with Mockito {
  class MyException extends WriteException(new Exception)

  "RetryingBroker" should {
    val exception = new MyException
    var invocations = 0
    val tries = 3
    val someMessage = mock[Object]
    val someReply = mock[Object]

    "when it never succeeds" in {
      val underlying = new Broker {
        def apply(request: AnyRef) = {
          invocations += 1
          Future.exception(exception)
        }
      }
      val retryingBroker = RetryingBroker.tries(underlying, tries)

      "retries up to $tries times" in {
        val f = retryingBroker(someMessage)

        f() must throwA(exception)
        invocations mustEqual 3
      }

      "apply retries on each new request" in {
        for (_ <- 0 until 3) {
          invocations = 0
          retryingBroker(someMessage)() must throwA(exception)
          invocations mustEqual 3
        }
      }
    }

    "when it eventually succeeds" in {
      val underlying = new Broker {
        def apply(request: AnyRef) = {
          invocations += 1
          if (invocations < 3) {
            Future.exception(exception)
          } else {
            Future.value(someReply)
          }
        }
      }
      val retryingBroker = RetryingBroker.tries(underlying, tries)

      "retries until it succeeds" in {
        retryingBroker(someMessage)() must be_==(someReply)
        invocations mustEqual 3
      }

      "apply retries on each new request" in {
        for (_ <- 0 until 3) {
          invocations = 0
          retryingBroker(someMessage)() must be_==(someReply)
          invocations mustEqual 3
        }
      }
    }

    "when it fails with a non-WriteException exception" in {
      val regularException = new Exception
      val underlying = new Broker {
        def apply(request: AnyRef) = {
          invocations += 1
          Future.exception(regularException)
        }
      }
      val retryingBroker = RetryingBroker.tries(underlying, tries)

      "never retry" in {
        retryingBroker(someMessage)() must throwA(regularException)
        invocations must be_==(1)
      }
    }
  }
}
