package com.twitter.finagle.util

import java.util.concurrent.ThreadFactory
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.verify
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class BlockingTimeTrackingThreadFactoryTest extends AnyFunSuite with MockitoSugar {

  private class RunnableCount extends Runnable {
    var runs = 0
    def run(): Unit =
      runs += 1
  }

  test("delegates to newRunnable and underlying ThreadFactory") {
    val threadFactory = mock[ThreadFactory]
    val ptf = new BlockingTimeTrackingThreadFactory(threadFactory)

    val r = new RunnableCount()
    ptf.newThread(r)
    assert(r.runs == 0)
    verify(threadFactory).newThread(any())
  }

}
