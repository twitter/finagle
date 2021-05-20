package com.twitter.finagle.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.server.StackServer
import com.twitter.finagle.stack.Endpoint
import com.twitter.util.{Await, Future}
import java.util.concurrent.atomic.AtomicInteger
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ServerAdmissionControlTest extends AnyFunSuite with MockitoSugar {
  class Ctx {
    val a = new AtomicInteger(1)

    class AdditionFilter[Req, Rep](delta: Int) extends SimpleFilter[Req, Rep] {
      def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
        a.addAndGet(delta)
        service(req)
      }
    }

    object Addition2Filter {
      val name = "multiple 2"

      val typeAgnostic: TypeAgnostic =
        new TypeAgnostic {
          override def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] =
            new AdditionFilter(2)
        }
    }

    object Addition3Filter {
      val name = "multiple 3"

      val typeAgnostic: TypeAgnostic =
        new TypeAgnostic {
          override def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] =
            new AdditionFilter(3)
        }
    }

    ServerAdmissionControl.unregisterAll()

    val echo = ServiceFactory.const(Service.mk[Int, Int](v => Future.value(v)))
    val stack = StackServer.newStack[Int, Int] ++ Stack.leaf(Endpoint, echo)

    ServerAdmissionControl.register(
      Addition2Filter.name,
      Addition2Filter.typeAgnostic
    )
  }

  test("register a controller") {
    val ctx = new Ctx
    import ctx._

    val factory = stack.make(StackServer.defaultParams)
    val svc = Await.result(factory(), 5.seconds)

    assert(Await.result(svc(1), 5.seconds) == 1)
    assert(a.get == 3)
  }

  test("disabled by param") {
    val ctx = new Ctx
    import ctx._

    val factory = stack.make(
      StackServer.defaultParams +
        ServerAdmissionControl.Param(false)
    )
    val svc = Await.result(factory(), 5.seconds)
    assert(Await.result(svc(1), 5.seconds) == 1)
    assert(a.get == 1)
  }

  test("unregister a controller") {
    val ctx = new Ctx
    import ctx._

    ServerAdmissionControl.unregister(Addition2Filter.name)

    val factory = stack.make(StackServer.defaultParams)
    val svc = Await.result(factory(), 5.seconds)

    assert(Await.result(svc(1), 5.seconds) == 1)
    assert(a.get == 1)
  }

  test("register multiple controller") {
    val ctx = new Ctx
    import ctx._

    ServerAdmissionControl.register(
      (Addition2Filter.name, Addition2Filter.typeAgnostic),
      (Addition3Filter.name, Addition3Filter.typeAgnostic)
    )

    val factory = stack.make(StackServer.defaultParams)
    val svc = Await.result(factory(), 5.seconds)

    assert(Await.result(svc(1), 5.seconds) == 1)
    assert(a.get == 6)
  }

  test("duplicated registration is ignored") {
    val ctx = new Ctx
    import ctx._

    ServerAdmissionControl.register(
      Addition2Filter.name,
      Addition2Filter.typeAgnostic
    )

    val factory = stack.make(StackServer.defaultParams)
    val svc = Await.result(factory(), 5.seconds)
    assert(Await.result(svc(1), 5.seconds) == 1)
    assert(a.get == 3)
  }

  test("Respects the NonRetryable context entry") {
    val ctx = new Ctx
    import ctx._

    val factory = stack.make(StackServer.defaultParams)
    val svc = Await.result(factory(), 5.seconds)

    Contexts.local.let(ServerAdmissionControl.NonRetryable, ()) {
      val aInitial = a.get
      assert(Await.result(svc(1), 5.seconds) == 1)
      assert(a.get == aInitial)
    }
  }
}
