package com.twitter.finagle.filter

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.Filter.TypeAgnostic
import com.twitter.finagle._
import com.twitter.finagle.context.Contexts
import com.twitter.finagle.filter.ServerAdmissionControl.ServerParams
import com.twitter.finagle.server.StackServer
import com.twitter.finagle.stack.Endpoint
import com.twitter.util.{Await, Future}
import java.util.concurrent.atomic.AtomicInteger
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class ServerAdmissionControlTest extends AnyFunSuite with MockitoSugar {

  class Ctx {
    val a = new AtomicInteger(1)

    class AdditionFilter(delta: Int) extends TypeAgnostic {
      val name = s"multiple $delta"

      override def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = new SimpleFilter[Req, Rep] {

        def apply(req: Req, service: Service[Req, Rep]): Future[Rep] = {
          a.addAndGet(delta)
          service(req)
        }
      }
    }

    def injectFilter(filter: AdditionFilter): Stackable[ServiceFactory[Int, Int]] =
      new Stack.TransformParams[ServiceFactory[Int, Int]] {
        private val head = Stack.Head(Stack.Role("addAc"))
        def transform(params: Stack.Params): Stack.Params = {
          val j: ServerParams => TypeAgnostic = _ => filter
          val nextFilters =
            params[ServerAdmissionControl.Filters].filters + (filter.name -> j)
          params + ServerAdmissionControl.Filters(nextFilters)
        }

        def role: Stack.Role = head.role

        def description: String = head.description

        def parameters: Seq[Stack.Param[_]] = head.parameters
      }

    val echo = ServiceFactory.const(Service.mk[Int, Int](v => Future.value(v)))
    val stack = StackServer
      .newStack[Int, Int].insertBefore(
        ServerAdmissionControl.role,
        injectFilter(new AdditionFilter(2))) ++
      Stack.leaf(Endpoint, echo)
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

  test("register multiple controller") {
    val ctx = new Ctx
    import ctx._

    val factory = stack
      .insertBefore(ServerAdmissionControl.role, injectFilter(new AdditionFilter(3)))
      .make(StackServer.defaultParams)

    val svc = Await.result(factory(), 5.seconds)

    assert(Await.result(svc(1), 5.seconds) == 1)
    assert(a.get == 6)
  }

  test("duplicated registration is ignored") {
    val ctx = new Ctx
    import ctx._

    val factory = stack
      .insertBefore(ServerAdmissionControl.role, injectFilter(new AdditionFilter(2))).make(
        StackServer.defaultParams)

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
