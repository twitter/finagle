package com.twitter.finagle

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.mockito.Mockito.{never, spy, times, verify}
import org.mockito.Matchers._
import com.twitter.finagle.service.{ConstantService, NilService}
import com.twitter.util.{Await, Future, Time}

@RunWith(classOf[JUnitRunner])
class FilterTest extends FunSuite {
  class PassThruFilter extends Filter[Int, Int, Int, Int] {
    def apply(req: Int, svc: Service[Int, Int]) = svc(req)
  }

  class PassThruFunction extends Function1[Int, Future[Int]] {
    def apply(req: Int) = Future.value(req)
  }

  class PassThruServiceFactory extends ServiceFactory[Int, Int] {
    def apply(conn: ClientConnection) = Future.value(constSvc)
    def close(deadline: Time) = Future.Done
  }

  val constSvc = new ConstantService[Int, Int](Future.value(2))

  test("Filter.andThen(Filter): applies next filter") {
    val spied = spy(new PassThruFilter)
    val svc = (new PassThruFilter).andThen(spied).andThen(constSvc)
    Await.result(svc(4))
    verify(spied).apply(any[Int], any[Service[Int, Int]])
  }

  test("Filter.andThen(Filter): lifts synchronous exceptions into Future.exception") {
    val fail = Filter.mk[Int, Int, Int, Int] { (_, _) => throw new Exception }
    val svc = (new PassThruFilter).andThen(fail).andThen(constSvc)
    val result = Await.result(svc(4).liftToTry)
    assert(result.isThrow)
  }

  test("Filter.andThen(Service): applies next filter") {
    val spied = spy(new ConstantService[Int, Int](Future.value(2)))
    val svc = (new PassThruFilter).andThen(spied)
    Await.result(svc(4))
    verify(spied).apply(any[Int])
  }

  test("Filter.andThen(Service): lifts synchronous exceptions into Future.exception") {
    val svc = (new PassThruFilter).andThen(NilService)
    val result = Await.result(svc(4).liftToTry)
    assert(result.isThrow)
  }

  test("Filter.andThen(Function1): applies next filter") {
    val spied = spy(new PassThruFunction)
    val svc = (new PassThruFilter).andThen(spied)
    Await.result(svc(4))
    verify(spied).apply(any[Int])
  }

  test("Filter.andThen(ServiceFactory): applies next filter") {
    val spied = spy(new PassThruServiceFactory)
    val sf = (new PassThruFilter).andThen(spied)
    Await.result(sf(ClientConnection.nil))
    verify(spied).apply(any[ClientConnection])
  }

  test("Filter.andThenIf: applies next filter when true") {
    val spied = spy(new PassThruFilter)
    val svc = (new PassThruFilter).andThenIf((true, spied)).andThen(constSvc)
    Await.result(svc(4))
    verify(spied).apply(any[Int], any[Service[Int, Int]])
  }

  test("Filter.andThenIf: doesn't apply next filter when false") {
    val spied = spy(new PassThruFilter)
    val svc = (new PassThruFilter).andThenIf((false, spied)).andThen(constSvc)
    Await.result(svc(4))
    verify(spied, never).apply(any[Int], any[Service[Int, Int]])
  }

  test("Filter.choose: apply the underlying filter to certain requests") {
    val spied = spy(new PassThruFilter)
    val svc = Filter.choose[Int, Int] {
      case req if req > 0 => spied
    }.andThen(constSvc)

    assert(Await.result(svc(100)) == 2)
    verify(spied, times(1)).apply(any[Int], any[Service[Int, Int]])

    assert(Await.result(svc(-99)) == 2)
    verify(spied, times(1)).apply(any[Int], any[Service[Int, Int]])
  }

  test("Filter.TypeAgnostic.toFilter distributes over Filter.TypeAgnostic.andThen") {
    val calls = List.newBuilder[(Any, Any)]
    class TestFilter extends Filter.TypeAgnostic {
      def toFilter[Req, Rep] =
        Filter.mk[Req, Rep, Req, Rep] { (req, svc) =>
          calls += (this -> req)
          svc(req)
        }
    }

    // A value that is equal only to itself
    object Input

    // Return an ordered list of TestFilters called and the requests
    // seen when evaluating f on an input.
    def getCalls(f: Filter[Any, Unit, Any, Unit]) = {
      calls.clear()
      f.andThen((_: Any) => Future.Done).apply(Input)
      calls.result
    }

    // Create two different filters that we will compose
    object F1 extends TestFilter
    object F2 extends TestFilter

    // This is the result that we expect, regardless of the nesting of
    // andThen and toFilter.
    val expected = List(F1 -> Input, F2 -> Input)

    // Get the calls produced by converting to filter and then composing
    val distributed = getCalls(F1.toFilter.andThen(F2.toFilter))

    // Get the calls produced by composing and then converting to filter
    val composed = getCalls(F1.andThen(F2).toFilter)

    assert(expected == composed)
    assert(distributed == composed)
  }

  test("Filter.TypeAgnostic.Identity is a left and right identity for andThen") {
    // The property that we want is that it is a left and right identity
    // *for the filter that is produced by toFilter*. We don't care
    // whether it is an identity at the TypeAgnostic level, because
    // TypeAgnostic filters can only be used by converting to a regular
    // filter.

    // Check whether the given function produces a TypeAgnostic filter
    // that is equivalent to its input.
    def isIdentity(f: Filter.TypeAgnostic => Filter.TypeAgnostic) = {
      // Since we have to specialize the filter for each call, we have
      // to create a new instance each time, which means we can't check
      // for identity using a straight .eq comparison. We make F final
      // so that we can use being an instance of the class as a proxy
      // for being returned from T.toFilter, and use that as a proxy for
      // identity.
      final class F[Req, Rep] extends Filter[Req, Rep, Req, Rep] {
        override def apply(req: Req, svc: Service[Req, Rep]) = svc(req)
      }

      object T extends Filter.TypeAgnostic {
        override def toFilter[Req, Rep] = new F[Req, Rep]
      }

      assert(f(T).toFilter.isInstanceOf[F[_, _]])
    }

    // Left identity
    isIdentity(Filter.TypeAgnostic.Identity.andThen(_))

    // Right identity
    isIdentity(_.andThen(Filter.TypeAgnostic.Identity))
  }
}
