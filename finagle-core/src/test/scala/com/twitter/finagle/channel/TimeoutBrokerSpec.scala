package com.twitter.finagle.channel

import org.specs.Specification
import org.specs.mock.Mockito
import com.twitter.conversions.time._
import com.twitter.util.Promise
import com.twitter.finagle.util.Timer

object TimeoutBrokerSpec extends Specification with Mockito {
  "TimeoutBroker" should {
    val timer = Timer.default
    val promise = new Promise[String]
    val broker = new Broker {
      def apply(request: AnyRef) = promise
    }
    val timeoutBroker = new TimeoutBroker(broker, 1.second, timer)

    "cancels the request when the service succeeds" in {
      promise.setValue("1")
      timeoutBroker("blah")(2.seconds) mustBe "1"
    }

    "times out a request that is not successful" in {
      timeoutBroker("blah")(2.seconds) must throwA[TimedoutRequestException]
    }
  }
}
