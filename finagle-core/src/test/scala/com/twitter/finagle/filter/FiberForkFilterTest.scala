package com.twitter.finagle.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.Stack
import com.twitter.util.Await
import com.twitter.util.Local
import com.twitter.util.Future
import org.scalatest.funsuite.AnyFunSuite

class FiberForkFilterTest extends AnyFunSuite {

  test("FiberForkFilter should modify the Local context") {
    val ctxAtStart: Local.Context = Local.save()
    var ctxInServiceOption: Option[Local.Context] = Option.empty
    val svc = Service.mk[Unit, Unit] { _ =>
      ctxInServiceOption = Some(Local.save())
      Future.Unit
    }

    FiberForkFilter.module
    val stack = FiberForkFilter
      .module[Unit, Unit]
      .toStack(Stack.leaf(Stack.Role("test"), ServiceFactory.const(svc)))
    Await.result(stack.make(Stack.Params.empty).toService(), 1.second)

    // The FiberForkFilter sets a new Fiber on the context so the Local context within the
    // service should not be equal to the initial Local context
    assert(ctxAtStart != ctxInServiceOption.get)
  }
}
