package com.twitter.finagle.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.Stack
import com.twitter.util.Await
import com.twitter.util.ExecutorWorkQueueFiber
import com.twitter.util.Fiber
import com.twitter.util.Local
import com.twitter.util.Future
import com.twitter.util.FuturePool
import com.twitter.util.SchedulerWorkQueueFiber
import java.util.concurrent.Executors
import org.scalatest.funsuite.AnyFunSuite

class FiberForkFilterTest extends AnyFunSuite {

  private val useWorkQueueToggleKey = "com.twitter.finagle.filter.UseWorkQueueFiber"

  test("FiberForkFilter should modify the Local context") {
    val ctxAtStart: Local.Context = Local.save()
    var ctxInServiceOption: Option[Local.Context] = Option.empty
    val svc = Service.mk[Unit, Unit] { _ =>
      ctxInServiceOption = Some(Local.save())
      Future.Unit
    }

    val stack = FiberForkFilter
      .module[Unit, Unit]
      .toStack(Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc)))
    Await.result(stack.make(Stack.Params.empty).toService(), 1.second)

    // The FiberForkFilter sets a new Fiber on the context so the Local context within the
    // service should not be equal to the initial Local context
    assert(ctxAtStart != ctxInServiceOption.get)
  }

  test("Work queueing fibers disabled by zone") {
    // restrict zone
    com.twitter.finagle.filter.restrictWorkQueueFiberZone.let(true) {
      // enable toggle, but it should not matter
      com.twitter.finagle.toggle.flag.overrides.let(useWorkQueueToggleKey, 1) {
        val fiberFactory = FiberForkFilter.generateFiberFactory(Option.empty)
        assert(fiberFactory.apply().getClass == Fiber.newCachedSchedulerFiber().getClass)
      }
    }
  }

  test("Work queue fibers disabled by toggle") {
    // allow all zones
    com.twitter.finagle.filter.restrictWorkQueueFiberZone.let(false) {
      // disable toggle
      com.twitter.finagle.toggle.flag.overrides.let(useWorkQueueToggleKey, 0) {
        val fiberFactory = FiberForkFilter.generateFiberFactory(Option.empty)
        assert(fiberFactory.apply().getClass == Fiber.newCachedSchedulerFiber().getClass)
      }
    }
  }

  test("Executor work queue fibers enabled by toggle") {
    // allow all zones
    com.twitter.finagle.filter.restrictWorkQueueFiberZone.let(false) {
      // enable toggle
      com.twitter.finagle.toggle.flag.overrides.let(useWorkQueueToggleKey, 1) {
        val fiberFactory =
          FiberForkFilter.generateFiberFactory(Some(FuturePool(Executors.newFixedThreadPool(1))))
        assert(fiberFactory.apply().isInstanceOf[ExecutorWorkQueueFiber])
      }
    }
  }

  test("Scheduler work queue fibers enabled by toggle") {
    // allow all zones
    com.twitter.finagle.filter.restrictWorkQueueFiberZone.let(false) {
      // enable toggle
      com.twitter.finagle.toggle.flag.overrides.let(useWorkQueueToggleKey, 1) {
        val fiberFactory = FiberForkFilter.generateFiberFactory(Option.empty)
        assert(fiberFactory.apply().isInstanceOf[SchedulerWorkQueueFiber])
      }
    }
  }
}
