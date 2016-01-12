package com.twitter.finagle.dispatch

import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.{Failure, WriteException}
import com.twitter.util.{Future, Promise, Return, Throw}
import org.junit.runner.RunWith
import org.mockito.Matchers._
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ClientDispatcherTest extends FunSuite with MockitoSugar {
  class DispatchHelper {
    val stats = new InMemoryStatsReceiver()
    val trans = mock[Transport[String, String]]
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

  test("ClientDispatcher queue_size gauge") {
    val h = new DispatchHelper
    import h._

    def assertGaugeSize(size: Int): Unit =
      assert(stats.gauges(Seq("serial", "queue_size"))() == size)

    assertGaugeSize(0)

    val p = new Promise[String]()
    when(trans.write(any[String])).thenReturn(Future.Done)
    when(trans.read()).thenReturn(p)

    disp("0")
    assertGaugeSize(0) // 1 issued, but none pending

    disp("1")
    disp("2")
    assertGaugeSize(2) // 1 issued, now 2 pending

    p.setValue("done")
    assertGaugeSize(0)
  }

}
