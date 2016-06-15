package com.twitter.finagle.dispatch

import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Promise, Future}
import com.twitter.finagle.transport.Transport
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito.{when, never, verify}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class PipeliningDispatcherTest extends FunSuite with MockitoSugar {
  test("PipeliningDispatcher: should treat interrupts properly") {
    val closeP = new Promise[Throwable]
    val trans = mock[Transport[Unit, Unit]]
    when(trans.write(())).thenReturn(Future.Done)
    when(trans.read()).thenReturn(Future.never)
    when(trans.onClose).thenReturn(closeP)
    val dispatch = new PipeliningDispatcher[Unit, Unit](trans)
    val f = dispatch(())
    f.raise(new Exception())
    verify(trans, never()).close()
  }

  test("queue_size gauge") {
    val stats = new InMemoryStatsReceiver()

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
    val dispatcher = new PipeliningDispatcher[String, String](trans, stats)

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
