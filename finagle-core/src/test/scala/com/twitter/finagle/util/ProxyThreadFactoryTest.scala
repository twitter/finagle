package com.twitter.finagle.util

import java.util.concurrent.ThreadFactory
import org.junit.runner.RunWith
import org.mockito.Matchers.any
import org.mockito.Mockito.verify
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class ProxyThreadFactoryTest extends FunSuite
  with MockitoSugar {

  private class RunnableCount extends Runnable {
    var runs = 0
    def run(): Unit =
      runs += 1
  }

  test("newProxiedRunnable") {
    var pre = 0
    var post = 0
    val mkProxy = ProxyThreadFactory.newProxiedRunnable(
      () => pre += 1,
      () => post += 1
    )

    val r = new RunnableCount()
    val newR = mkProxy(r)
    newR.run()
    assert(pre == 1)
    assert(post == 1)
    assert(r.runs == 1)
  }

  test("delegates to newRunnable and underlying ThreadFactory") {
    var created = 0
    val newR: Runnable => Runnable = { r =>
      created += 1
      r
    }

    val threadFactory = mock[ThreadFactory]
    val ptf = new ProxyThreadFactory(threadFactory, newR)

    val r = new RunnableCount()
    ptf.newThread(r)
    assert(r.runs == 0)
    assert(created == 1)
    verify(threadFactory).newThread(any())
  }

}
