package com.twitter.finagle.load

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.util.{Duration, Future, MockTimer, Promise, Time, TimeControl, Try}
import com.twitter.conversions.time.intToTimeableNumber
import scala.collection.immutable.SortedSet

class LoadGenerator[Req, Rep](
  history: TraversableOnce[Event[Req, Rep]],
  recorder: (Duration, Future[Rep]) => Unit,
  filter: SimpleFilter[Event[Req, Rep], Rep],
  timer: MockTimer = new MockTimer()
) {
  val svc = filter andThen Service.mk[Event[Req, Rep], Rep] { evt: Event[Req, Rep] =>
    val p = Promise[Rep]()
    timer.schedule(evt.finish) {
      p.updateIfEmpty(evt())
    }
    p
  }


  def forward(dur: Duration, ctl: TimeControl) {
    ctl.advance(dur - 1.nanosecond)
    timer.tick()
    ctl.advance(1.nanosecond)
    timer.tick()
  }

  /**
   * Makes a request to a filtered service that starts at Event.start and lasts for Event.length,
   * and at the end, fulfils the response with the result of Event.fn(Event.length).
   *
   * Every time a request is made, it is recorded with the latency and passed to recorder.
   */
  def execute(): Unit = {
    var cur = Time.fromMilliseconds(0)
    var endTimes = SortedSet[Time]()

    // sets time to one nanosecond before the end of the next request
    // then sets time to the precise nanosecond of the end of the next request
    // this is done to ensure that if there is something affecting the satisfaction
    // of a promise, that it is triggered before the promise is satisfied.
    // sort of a hack
    def removeInterstices(end: Time, ctl: TimeControl) {
      val removees = endTimes.rangeImpl(None, Some(end))
      for (ts <- removees) {
        if (ts - cur > Duration.Zero) {
          forward(ts - cur, ctl)
          cur = ts
        }
      }
      endTimes --= removees
      forward(end - cur, ctl)
      cur = end
    }

    var maxSeen = cur
    Time.withTimeAt(cur) { ctl =>
      history foreach { evt =>
        val diff = (evt.start - cur)
        if (diff > Duration.Zero) {
          removeInterstices(evt.start, ctl)
        }
        cur = evt.start
        endTimes += (evt.finish)
        recorder(evt.length, svc(evt))
        maxSeen = maxSeen max (evt.finish)
      }
      removeInterstices(maxSeen, ctl)
    }
  }
}

object LoadGenerator {
  def mkHistory[Req, Rep](event: () => Event[Req, Rep], num: Int): Iterator[Event[Req, Rep]] =
    new Iterator[Event[Req, Rep]] {
      def hasNext: Boolean = true
      def next(): Event[Req, Rep] = event()
    }.take(num)

  def mk[Req, Rep](
    start: Time,
    interval: Duration,
    mkEvent: Time => Event[Req, Rep],
    num: Int
  ): Iterator[Event[Req, Rep]] = mkInGroups(start, interval, mkEvent, 1, num)

  def mkInGroups[Req, Rep](
    start: Time,
    interval: Duration,
    mkEvent: Time => Event[Req, Rep],
    groupSize: Int,
    num: Int
  ): Iterator[Event[Req, Rep]] = {
    var cur = start
    var curGroupSize = 0
    mkHistory({ () =>
      val evt = mkEvent(cur)
      curGroupSize += 1
      if (curGroupSize == groupSize) {
        curGroupSize = 0
        cur += interval
      }
      evt
    }, num)
  }
}
