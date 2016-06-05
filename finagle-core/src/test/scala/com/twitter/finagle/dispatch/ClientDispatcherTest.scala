package com.twitter.finagle.dispatch

import com.twitter.conversions.time._
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Failure, WriteException}
import com.twitter.util._
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito.{times, verify, when, never}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ClientDispatcherTest extends FunSuite with MockitoSugar {
  class DispatchHelper {
    val closeP = new Promise[Exception]
    val stats = new InMemoryStatsReceiver()
    val trans = mock[Transport[String, String]]
    when(trans.onClose).thenReturn(closeP)

    val disp = new SerialClientDispatcher[String, String](trans, stats)
  }

  test("ClientDispatcher should dispatch requests") {
    val h = new DispatchHelper
    import h._

    when(trans.write("one")) thenReturn Future.value(())
    val p = new Promise[String]
    when(trans.read()) thenReturn p
    val f = disp("one")
    verify(trans).write("one")
    verify(trans).read()

    assert(!f.isDefined)
    p.setValue("ok: one")
    assert(f.poll == Some(Return("ok: one")))
  }

  test("ClientDispatcher should dispatch requests one-at-a-time") {
    val h = new DispatchHelper
    import h._

    when(trans.write(any[String])) thenReturn Future.value(())
    val p0, p1 = new Promise[String]
    when(trans.read()) thenReturn p0
    val f0 = disp("one")
    verify(trans).write(any[String])
    verify(trans).read()
    val f1 = disp("two")
    verify(trans).write(any[String])
    verify(trans).read()

    assert(!f0.isDefined)
    assert(!f1.isDefined)

    when(trans.read()) thenReturn p1
    p0.setValue("ok: one")
    assert(f0.poll == Some(Return("ok: one")))
    verify(trans, times(2)).write(any[String])
    verify(trans, times(2)).read()

    assert(!f1.isDefined)
    p1.setValue("ok: two")
    assert(p1.poll == Some(Return("ok: two")))
  }

  test("ClientDispatcher should interrupt when close transport and cancel pending requests") {
    val h = new DispatchHelper
    import h._

    when(trans.write(any[String])) thenReturn Future.value(())
    val p0 = new Promise[String]
    when(trans.read()) thenReturn p0
    val f0 = disp("zero")
    val f1 = disp("one")
    verify(trans).write("zero")
    verify(trans).read()
    assert(!f0.isDefined)
    assert(!f1.isDefined)

    val intr = new Exception
    f0.raise(intr)
    verify(trans).close()
    assert(f0.poll == Some(Throw(intr)))
  }

  test("ClientDispatcher should interrupt when ignore pending") {
    val h = new DispatchHelper
    import h._

    when(trans.write(any[String])) thenReturn Future.value(())
    val p0 = new Promise[String]
    when(trans.read()) thenReturn p0
    val f0 = disp("zero")
    val f1 = disp("one")
    verify(trans).write("zero")
    verify(trans).read()
    assert(!f0.isDefined)
    assert(!f1.isDefined)

    val intr = new Exception
    f1.raise(intr)
    verify(trans, times(0)).close()
    assert(!f0.isDefined)
    assert(!f1.isDefined)

    p0.setValue("ok")
    assert(f0.poll == Some(Return("ok")))
    assert(f1.poll == Some(Throw(Failure(intr, Failure.Interrupted))))
    verify(trans).write(any[String])
  }

  test("ClientDispatcher should rewrite WriteExceptions") {
    val h = new DispatchHelper
    import h._

    val exc = mock[Exception]
    when(trans.write(any[String])) thenReturn Future.exception(exc)
    val resultOpt = disp("hello").poll

    assert(resultOpt.isDefined)
    assert(resultOpt.get.isThrow)

    val result: Throwable = resultOpt.get.asInstanceOf[Throw[String]].e
    assert(result.isInstanceOf[WriteException])
    assert(result.getCause == exc)
  }

  def assertGaugeSize(stats: InMemoryStatsReceiver, size: Int): Unit =
    assert(stats.gauges(Seq("serial", "queue_size"))() == size)

  test("ClientDispatcher queue_size gauge") {
    val h = new DispatchHelper
    import h._


    assertGaugeSize(stats, 0)

    val p = new Promise[String]()
    when(trans.write(any[String])).thenReturn(Future.Done)
    when(trans.read()).thenReturn(p)

    disp("0")
    assertGaugeSize(stats, 0) // 1 issued, but none pending

    disp("1")
    disp("2")
    assertGaugeSize(stats, 2) // 1 issued, now 2 pending

    p.setValue("done")
    assertGaugeSize(stats, 0)
  }

  test("pending dispatches are failed on transport close") {
    val h = new DispatchHelper
    import h._

    when(trans.write(any[String])).thenReturn(Future.never)
    val (r1, r2, r3) = (disp("0"), disp("1"), disp("2"))

    // first request receives the permit, the write never returns
    // subsequent requests are queued.
    assertGaugeSize(stats, 2)

    closeP.setException(new Exception("fin"))

    // pending requests are failed
    val e1 = intercept[Exception] { (Await.result(r2, 2.seconds)) }
    val e2 = intercept[Exception] { (Await.result(r3, 2.seconds)) }

    assert(e1.getMessage == "fin")
    assert(e2.getMessage == "fin")
  }

  test("dispatcher with a closed transport fails fast") {
    val h = new DispatchHelper
    import h._

    closeP.setException(new Exception("fin"))
    val (r1, r2, r3) = (disp("0"), disp("1"), disp("2"))

    // requests are failed
    val e1 = intercept[Exception] { (Await.result(r1, 2.seconds)) }
    val e2 = intercept[Exception] { (Await.result(r2, 2.seconds)) }
    val e3 = intercept[Exception] { (Await.result(r3, 2.seconds)) }

    assert(e1.getMessage == "fin")
    assert(e2.getMessage == "fin")
    assert(e3.getMessage == "fin")

    // transport never sees write
    verify(trans, never).write(any[String])
    verify(trans, never).read()
  }
}
