package com.twitter.finagle.netty4.ssl

import com.twitter.conversions.DurationOps._
import com.twitter.util.{FuturePool, MockTimer, Time}
import io.netty.handler.ssl.SslContext
import java.util.concurrent.atomic.AtomicInteger
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ContextReloaderTest extends AnyFunSuite with MockitoSugar {

  test("it polls ctxFac") {
    val ctxFacCalled = new AtomicInteger()
    val ctx = mock[SslContext]
    def ctxFac: SslContext = {
      ctxFacCalled.incrementAndGet()
      ctx
    }

    Time.withCurrentTimeFrozen { ctx =>
      val timer = new MockTimer
      assert(ctxFacCalled.get() == 0)
      val reloader = new ContextReloader(
        ctxFac,
        timer,
        reloadPeriod = 10.seconds,
        FuturePool.immediatePool
      )
      assert(ctxFacCalled.get() == 1)

      // not yet
      ctx.advance(9.seconds)
      assert(ctxFacCalled.get() == 1)
      timer.tick()

      // 10 seconds elapsed
      ctx.advance(1.seconds)
      timer.tick()
      assert(ctxFacCalled.get() == 2)

      ctx.advance(10.seconds)
      timer.tick()
      assert(ctxFacCalled.get() == 3)
    }
  }

  test("resiliency to non-fatal exceptions in polling update") {
    val ctxFacCalled = new AtomicInteger()
    val ctx = mock[SslContext]
    def ctxFac: SslContext = {
      if (ctxFacCalled.incrementAndGet() >= 2) throw new Exception("fac failure")
      ctx
    }

    Time.withCurrentTimeFrozen { ctx =>
      val timer = new MockTimer
      assert(ctxFacCalled.get() == 0)
      val reloader = new ContextReloader(
        ctxFac,
        timer,
        reloadPeriod = 10.seconds,
        FuturePool.immediatePool
      )
      timer.tick()
      assert(ctxFacCalled.get() == 1)

      ctx.advance(10.seconds)
      timer.tick()
      assert(ctxFacCalled.get() == 2)

      ctx.advance(10.seconds)
      timer.tick()
      assert(ctxFacCalled.get() == 3)
      assert(reloader.sslContext != null)
    }
  }
}
