package com.twitter.finagle.dispatch

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.{IndividualRequestTimeoutException => FinagleTimeoutException}
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.stats.NullStatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.transport.TransportContext
import com.twitter.util.Promise
import com.twitter.util.Future
import com.twitter.util.Time
import com.twitter.util.MockTimer
import com.twitter.util.{TimeoutException => UtilTimeoutException}
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito
import org.mockito.Mockito.never
import org.mockito.Mockito.verify
import org.mockito.Mockito.times
import org.mockito.stubbing.OngoingStubbing
import org.scalatestplus.mockito.MockitoSugar
import scala.language.reflectiveCalls
import org.scalatest.funsuite.AnyFunSuite

class PipeliningDispatcherTest extends AnyFunSuite with MockitoSugar {

  // Don't let the Scala compiler get confused about which `thenReturn`
  // method we want to use.
  private[this] def when[T](o: T) =
    Mockito.when(o).asInstanceOf[{ def thenReturn[RT](s: RT): OngoingStubbing[RT] }]

  val exns = Seq(
    ("util", new UtilTimeoutException("boom!"), never()),
    ("finagle", new FinagleTimeoutException(1.second), never())
  )

  exns.foreach {
    case (kind, exc, numClosed) =>
      test(s"PipeliningDispatcher: should ignore $kind timeout interrupts immediately") {
        val timer = new MockTimer
        Time.withCurrentTimeFrozen { _ =>
          val trans = mock[Transport[Unit, Unit]]
          val context = mock[TransportContext]
          when(trans.context).thenReturn(context)
          when(trans.write(())).thenReturn(Future.Done)
          when(trans.read()).thenReturn(Future.never)
          when(trans.onClose).thenReturn(Future.never)
          val dispatch =
            new PipeliningDispatcher[Unit, Unit](trans, NullStatsReceiver, 10.seconds, timer)
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
      val context = mock[TransportContext]
      when(trans.context).thenReturn(context)
      when(trans.write(())).thenReturn(Future.Done)
      when(trans.read()).thenReturn(Future.never)
      when(trans.onClose).thenReturn(Future.never)
      val dispatch =
        new PipeliningDispatcher[Unit, Unit](trans, NullStatsReceiver, 10.seconds, timer)
      val f = dispatch(())
      f.raise(new UtilTimeoutException("boom!"))
      assert(!f.isDefined)
    }
  }

  test("PipeliningDispatcher: should actually handle timeout interrupts after waiting") {
    val stallTimeout = 10.seconds
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { ctl =>
      val trans = mock[Transport[Unit, Unit]]
      val context = mock[TransportContext]
      when(trans.context).thenReturn(context)
      when(trans.write(())).thenReturn(Future.Done)
      when(trans.read()).thenReturn(Future.never)
      when(trans.onClose).thenReturn(Future.never)
      val dispatch =
        new PipeliningDispatcher[Unit, Unit](trans, NullStatsReceiver, stallTimeout, timer)
      val f = dispatch(())
      f.raise(new UtilTimeoutException("boom!"))
      verify(trans, never()).close()

      ctl.advance(stallTimeout)
      timer.tick()
      verify(trans, times(1)).close()
    }
  }

  test("PipeliningDispatcher: should not handle interrupts after waiting if the pipeline clears") {
    val stallTimeout = 10.seconds
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { ctl =>
      val trans = mock[Transport[Unit, Unit]]
      val context = mock[TransportContext]
      when(trans.context).thenReturn(context)
      val readP = Promise[Unit]()
      when(trans.write(())).thenReturn(Future.Done)
      when(trans.read()).thenReturn(readP)
      when(trans.onClose).thenReturn(Future.never)
      val dispatch =
        new PipeliningDispatcher[Unit, Unit](trans, NullStatsReceiver, stallTimeout, timer)
      val f = dispatch(())
      f.raise(new UtilTimeoutException("boom!"))
      verify(trans, never()).close()
      readP.setDone()

      ctl.advance(stallTimeout)
      timer.tick()
      verify(trans, never()).close()
    }
  }

  test("queue size") {
    val stats = new InMemoryStatsReceiver()
    val timer = new MockTimer

    val p0, p1, p2 = new Promise[String]()
    val trans = mock[Transport[String, String]]
    val context = mock[TransportContext]
    when(trans.context).thenReturn(context)
    when(trans.write(any[String])).thenReturn(Future.Done)
    when(trans.read())
      .thenReturn(p0)
      .thenReturn(p1)
      .thenReturn(p2)
    val closeP = new Promise[Throwable]
    when(trans.onClose).thenReturn(closeP)
    val dispatcher = new PipeliningDispatcher[String, String](trans, stats, 10.seconds, timer)

    assert(dispatcher.queueSize == 0)

    // issue 3 pipelined requests that immediately get
    // written to the transport, and thus put into the queue.
    // at the same time, the 1st element is removed from the queue
    // and the next read will not proceed until that one is fulfilled.
    dispatcher("0")
    dispatcher("1")
    dispatcher("2")
    assert(dispatcher.queueSize == 2) // as noted above, the "0" has been removed from the queue

    // then even if we fulfil them out of order...
    p2.setValue("2")
    assert(dispatcher.queueSize == 2)

    // this will complete 0, triggering 1 to be removed from the q.
    p0.setValue("0")
    assert(dispatcher.queueSize == 1)

    p1.setValue("1")
    assert(dispatcher.queueSize == 0)
  }
}
