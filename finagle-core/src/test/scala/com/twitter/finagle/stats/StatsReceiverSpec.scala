package com.twitter.finagle.stats

import com.twitter.conversions.time._
import com.twitter.util.Future
import java.util.concurrent.TimeUnit
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito

class StatsReceiverSpec extends SpecificationWithJUnit with Mockito {

  "StatsReceiver" should {
    "time" in {
      val receiver = spy(new InMemoryStatsReceiver)

      receiver.time("er", "mah", "gerd") { () }
      there was one(receiver).stat("er", "mah", "gerd")

      receiver.time(TimeUnit.NANOSECONDS, "er", "mah", "gerd") { () }
      there were two(receiver).stat("er", "mah", "gerd")

      val stat = receiver.stat("er", "mah", "gerd")
      there were three(receiver).stat("er", "mah", "gerd")
      receiver.time(TimeUnit.DAYS, stat) { () }
      there were three(receiver).stat("er", "mah", "gerd")
    }

    "timeFuture" in {
      val receiver = spy(new InMemoryStatsReceiver)

      (receiver.timeFuture("2", "chainz") { Future.Unit })(1.second)
      there was one(receiver).stat("2", "chainz")

      (receiver.timeFuture(TimeUnit.MINUTES, "2", "chainz") { Future.Unit })(1.second)
      there were two(receiver).stat("2", "chainz")

      val stat = receiver.stat("2", "chainz")
      there were three(receiver).stat("2", "chainz")
      (receiver.timeFuture(TimeUnit.HOURS, stat) { Future.Unit })(1.second)
      there were three(receiver).stat("2", "chainz")
    }
  }

}
