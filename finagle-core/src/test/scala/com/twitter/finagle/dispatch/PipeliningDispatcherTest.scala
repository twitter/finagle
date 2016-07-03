package com.twitter.finagle.dispatch

import com.twitter.conversions.time._
import com.twitter.finagle.{IndividualRequestTimeoutException => FinagleTimeoutException}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Promise, Future, Time, MockTimer, TimeoutException => UtilTimeoutException}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito.{when, never, verify, times}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class PipeliningDispatcherTest extends FunSuite with MockitoSugar {
  val exns = Seq(
    ("util", new UtilTimeoutException("boom!"), never()),
    ("finagle", new FinagleTimeoutException(1.second), never()))

  exns.foreach { case (kind, exc, numClosed) =>
    test(s"PipeliningDispatcher: should ignore $kind timeout interrupts immediately") {
      val timer = new MockTimer
      Time.withCurrentTimeFrozen { _ =>
        val trans = mock[Transport[Unit, Unit]]
        when(trans.write(())).thenReturn(Future.Done)
        when(trans.read()).thenReturn(Future.never)
        when(trans.onClose).thenReturn(Future.never)
        val dispatch = new PipeliningDispatcher[Unit, Unit](trans, NullStatsReceiver, timer)
        val f = dispatch(())
        f.raise(exc)
        verify(trans, numClosed).close()
      }
    }
  }

  test("PipeliningDispatcher: should not fail the request on an interrupt") {
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { ctl =>
      val trans = mock[Transport[Unit, Unit]]
      when(trans.write(())).thenReturn(Future.Done)
      when(trans.read()).thenReturn(Future.never)
      when(trans.onClose).thenReturn(Future.never)
      val dispatch = new PipeliningDispatcher[Unit, Unit](trans, NullStatsReceiver, timer)
      val f = dispatch(())
      f.raise(new UtilTimeoutException("boom!"))
      assert(!f.isDefined)
    }
  }

  test("PipeliningDispatcher: should actually handle timeout interrupts after waiting") {
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { ctl =>
      val trans = mock[Transport[Unit, Unit]]
      when(trans.write(())).thenReturn(Future.Done)
      when(trans.read()).thenReturn(Future.never)
      when(trans.onClose).thenReturn(Future.never)
      val dispatch = new PipeliningDispatcher[Unit, Unit](trans, NullStatsReceiver, timer)
      val f = dispatch(())
      f.raise(new UtilTimeoutException("boom!"))
      verify(trans, never()).close()

      ctl.advance(PipeliningDispatcher.TimeToWaitForStalledPipeline)
      timer.tick()
      verify(trans, times(1)).close()
    }
  }

  test("PipeliningDispatcher: should not handle interrupts after waiting if the pipeline clears") {
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { ctl =>
      val trans = mock[Transport[Unit, Unit]]
      val readP = Promise[Unit]()
      when(trans.write(())).thenReturn(Future.Done)
      when(trans.read()).thenReturn(readP)
      when(trans.onClose).thenReturn(Future.never)
      val dispatch = new PipeliningDispatcher[Unit, Unit](trans, NullStatsReceiver, timer)
      val f = dispatch(())
      f.raise(new UtilTimeoutException("boom!"))
      verify(trans, never()).close()
      readP.setDone()

      ctl.advance(PipeliningDispatcher.TimeToWaitForStalledPipeline)
      timer.tick()
      verify(trans, never()).close()
    }
  }

  test("queue_size gauge") {
    val stats = new InMemoryStatsReceiver()
    val timer = new MockTimer

    def assertGaugeSize(size: Int): Unit =
      assert(size == stats.gauges(Seq("pipelining", "pending"))())

    val p0, p1, p2 = new Promise[String]()
    val trans = mock[Transport[String, String]]
    when(trans.write(any[String])).thenReturn(Future.Done)
    when(trans.read())
      .thenReturn(p0)
      .thenReturn(p1)
      .thenReturn(p2)
    val closeP = new Promise[Throwable]
    when(trans.onClose).thenReturn(closeP)
    val dispatcher = new PipeliningDispatcher[String, String](trans, stats, timer)

    assertGaugeSize(0)

    // issue 3 pipelined requests that immediately get
    // written to the transport, and thus put into the queue.
    // at the same time, the 1st element is removed from the queue
    // and the next read will not proceed until that one is fulfilled.
    dispatcher("0")
    dispatcher("1")
    dispatcher("2")
    assertGaugeSize(2) // as noted above, the "0" has been removed from the queue

    // then even if we fulfil them out of order...
    p2.setValue("2")
    assertGaugeSize(2)

    // this will complete 0, triggering 1 to be removed from the q.
    p0.setValue("0")
    assertGaugeSize(1)

    p1.setValue("1")
    assertGaugeSize(0)
  }
}
