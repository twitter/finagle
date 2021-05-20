package com.twitter.finagle.pushsession

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.pushsession.utils.MockChannelHandle
import com.twitter.finagle.{Failure, IndividualRequestTimeoutException => FinagleTimeoutException}
import com.twitter.finagle.stats.{InMemoryStatsReceiver, NullStatsReceiver}
import com.twitter.util.{Await, MockTimer, Promise, Time, TimeoutException => UtilTimeoutException}
import java.net.{InetSocketAddress, SocketAddress}
import org.mockito.Mockito.never
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class PipeliningMockChannelHandle[In, Out] extends MockChannelHandle[In, Out] {

  // The remote address is logged when the pipeline stalls
  override val remoteAddress: SocketAddress = new InetSocketAddress("1.2.3.4", 100)
}

class PipeliningClientPushSessionTest extends AnyFunSuite with MockitoSugar {

  val exns = Seq(
    ("util", new UtilTimeoutException("boom!"), never()),
    ("finagle", new FinagleTimeoutException(1.second), never())
  )

  exns.foreach {
    case (kind, exc, numClosed) =>
      test(s"Should ignore $kind timeout interrupts immediately") {
        val timer = new MockTimer
        Time.withCurrentTimeFrozen { _ =>
          val handle = new PipeliningMockChannelHandle[Unit, Unit]()
          val session =
            new PipeliningClientPushSession[Unit, Unit](
              handle,
              NullStatsReceiver,
              10.seconds,
              timer
            ).toService
          val f = session(())
          f.raise(exc)
          assert(!handle.closedCalled)
        }
      }
  }

  test("Should not fail the request on an interrupt") {
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { ctl =>
      val handle = new PipeliningMockChannelHandle[Unit, Unit]()
      val service =
        new PipeliningClientPushSession[Unit, Unit](
          handle,
          NullStatsReceiver,
          10.seconds,
          timer
        ).toService
      val f = service(())
      f.raise(new UtilTimeoutException("boom!"))
      assert(!f.isDefined)
    }
  }

  test("Should handle timeout interrupts after waiting `stallTimeout`") {
    val stallTimeout = 10.seconds
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { ctl =>
      val handle = new PipeliningMockChannelHandle[Unit, Unit]()
      val service =
        new PipeliningClientPushSession[Unit, Unit](
          handle,
          NullStatsReceiver,
          stallTimeout,
          timer
        ).toService
      val f = service(())
      f.raise(new UtilTimeoutException("boom!"))
      assert(!handle.closedCalled)

      ctl.advance(stallTimeout)
      timer.tick()
      handle.serialExecutor.executeAll()
      assert(handle.closedCalled)
      val failure = intercept[Failure] {
        Await.result(f, 5.seconds)
      }
      assert(failure.why.contains("The connection pipeline could not make progress"))
    }
  }

  test("Should not handle interrupts after waiting if the pipeline clears") {
    val stallTimeout = 10.seconds
    val timer = new MockTimer
    Time.withCurrentTimeFrozen { ctl =>
      val handle = new PipeliningMockChannelHandle[Unit, Unit]()
      val session =
        new PipeliningClientPushSession[Unit, Unit](
          handle,
          NullStatsReceiver,
          stallTimeout,
          timer
        )
      val service = session.toService
      val f = service(())
      f.raise(new UtilTimeoutException("boom!"))
      assert(!handle.closedCalled)
      handle.serialExecutor.executeAll()
      session.receive(())
      ctl.advance(stallTimeout)
      timer.tick()
      assert(!handle.closedCalled)
    }
  }

  test("queue size") {
    val stats = new InMemoryStatsReceiver()
    val timer = new MockTimer

    var p0, p1, p2 = new Promise[String]()
    val handle = new PipeliningMockChannelHandle[String, String]()
    val session =
      new PipeliningClientPushSession[String, String](
        handle,
        stats,
        10.seconds,
        timer
      )
    val service = session.toService
    assert(session.getQueueSize == 0)

    service("0")
    service("1")
    service("2")
    handle.serialExecutor.executeAll()
    assert(session.getQueueSize == 3)

    session.receive("resp")
    assert(session.getQueueSize == 2)

    session.receive("resp")
    assert(session.getQueueSize == 1)

    session.receive("resp")
    assert(session.getQueueSize == 0)
  }

}
