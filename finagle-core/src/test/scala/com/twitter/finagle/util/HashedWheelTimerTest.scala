package com.twitter.finagle.util

import com.twitter.util.TimerTask
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import org.jboss.netty.{util => nu}
import org.mockito.ArgumentCaptor
import com.twitter.util.TimeConversions._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.{verify, when, atMost}
import org.mockito.Matchers._
import org.mockito.stubbing.Answer
import org.mockito.invocation.InvocationOnMock

@RunWith(classOf[JUnitRunner])
class HashedWheelTimerTest extends FunSuite with MockitoSugar {
  test("HashedWheelTimer should Support cancelling recurring tasks") {
    val timer = mock[nu.Timer]
    val nstop = new AtomicInteger(0)
    @volatile var running = true
    when(timer.stop()) thenAnswer {
      new Answer[java.util.Set[Nothing]] {
        override def answer(invocation: InvocationOnMock): java.util.Set[Nothing] = {
          running = false
          nstop.incrementAndGet()
          Collections.emptySet()
        }
      }
    }

    val t = new HashedWheelTimer(timer)

    val taskCaptor = ArgumentCaptor.forClass(classOf[nu.TimerTask])
    val firstTimeout = mock[nu.Timeout]
    when(firstTimeout.isCancelled) thenReturn false
    when(timer.newTimeout(taskCaptor.capture(), any[Long], any[java.util.concurrent.TimeUnit])) thenReturn firstTimeout

    var task: TimerTask = null
    task = t.schedule(1.second) {
      task.cancel()
    }

    taskCaptor.getValue.run(firstTimeout)

    verify(timer, atMost(1)).newTimeout(any[org.jboss.netty.util.TimerTask], any[Long], any[java.util.concurrent.TimeUnit])
  }

  // ko todo: add test for locals
}
