package com.twitter.finagle.netty4.util

import com.twitter.conversions.DurationOps._
import com.twitter.util.Duration
import com.twitter.util.Time
import io.netty.util.HashedWheelTimer
import io.netty.util.Timeout
import io.netty.util.Timer
import io.netty.util.TimerTask
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.scalatest.concurrent.Eventually
import scala.concurrent.duration.TimeUnit
import org.scalatest.funsuite.AnyFunSuite

class Netty4TimerTest extends AnyFunSuite with MockitoSugar with Eventually {

  test("schedule once") {
    var scheduled = false
    val timer = new Netty4Timer(new HashedWheelTimer())
    val task = timer.schedule(Time.now + 300.milliseconds) { scheduled = true }

    eventually(scheduled)
    task.cancel()
    timer.stop()
  }

  test("schedule periodically") {
    var scheduled = 0
    val timer = new Netty4Timer(new HashedWheelTimer())
    val task = timer.schedule(150.milliseconds) { scheduled += 1 }

    eventually(scheduled >= 2)
    task.cancel()
    timer.stop()
  }

  test("propagate cancel") {
    val underlying = mock[Timer]
    val timer = new Netty4Timer(underlying)

    val timeout = mock[Timeout]
    when(underlying.newTimeout(any[TimerTask], any[Long], any[TimeUnit])).thenReturn(timeout)

    // Cancel both tasks.
    timer.schedule(Time.Top)(()).cancel()
    timer.schedule(Duration.Top)(()).cancel()
    verify(timeout, times(2)).cancel()
  }

  test("propagate stop") {
    val underlying = mock[Timer]
    val timer = new Netty4Timer(underlying)

    timer.stop()
    verify(underlying, times(1)).stop()
  }
}
