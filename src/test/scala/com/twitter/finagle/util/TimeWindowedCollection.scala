package com.twitter.finagle.util

import scala.collection.mutable.ArrayBuffer
import org.specs.Specification
import org.specs.mock.Mockito

import com.twitter.util.{Duration, Time, TimeControl}
import com.twitter.util.TimeConversions._

object TimeWindowedCollectionSpec extends Specification with Mockito {
  "TimeWindowedCollection" should {
    val c = new TimeWindowedCollection[ArrayBuffer[Int]](10, 10.seconds)

    def exercise(control: TimeControl) {
      c() must beEmpty
      for (i <- 1 to 100) {
        c() += i
        control.advance(1.second)
      }
    }

    "data is stored in the correct buckets as time progresses and falls off" in {
      Time.withCurrentTimeFrozen { control =>
        exercise(control)
        val a = c.iterator.toArray
        a.head must contain(100)
        a.last must contain(1)
      }
    }

    "as the window shifts, data is garbage collected" in {
      Time.withCurrentTimeFrozen { control =>
        exercise(control)
        control.advance(10.seconds)
        val it = c.iterator
        var e: ArrayBuffer[Int] = null
        while(it.hasNext()) e = it.next()
        e mustNot contain(1)
      }
    }

    "timeSpan spands earliest to latest dates in the window" in {
      Time.withCurrentTimeFrozen { control =>
        def difference(span: (Time, Time)) = span match {
          case (begin: Time, end: Time) => (end - begin).inSeconds
          case _ => 0
        }
         
        for (i <- 1 until 10) {
          c() += i
          difference(c.timeSpan) mustEqual i * 10
          control.advance(10.seconds)
        }
      }
    }
  }
}
