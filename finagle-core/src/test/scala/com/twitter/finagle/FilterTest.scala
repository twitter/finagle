package com.twitter.finagle

import com.twitter.conversions.DurationOps._
import com.twitter.finagle.service.ConstantService
import com.twitter.finagle.service.NilService
import com.twitter.util.Await
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Time
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import org.mockito.Matchers._
import org.mockito.Mockito.never
import org.mockito.Mockito.spy
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.scalatest.funsuite.AnyFunSuite

class FilterTest extends AnyFunSuite {
  private def await[T](f: Future[T]): T = Await.result(f, 5.seconds)

  class PassThruFilter extends Filter[Int, Int, Int, Int] {
    def apply(req: Int, svc: Service[Int, Int]): Future[Int] = svc(req)
  }

  class PassThruTypeAgnosticFilter extends Filter.TypeAgnostic {
    def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = new Filter[Req, Rep, Req, Rep] {
      def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = service(request)
    }
  }

  class PassThruServiceFactory extends ServiceFactory[Int, Int] {
    def apply(conn: ClientConnection): Future[Service[Int, Int]] = Future.value(constSvc)
    def close(deadline: Time): Future[Unit] = Future.Done
    def status: Status = Status.Open
  }

  val constSvc = new ConstantService[Int, Int](Future.value(2))
  val constSvcFactory = ServiceFactory.const(constSvc)

  class FilterPlus1 extends Filter[Int, Int, Int, Int] {
    def apply(request: Int, service: Service[Int, Int]): Future[Int] = Future.value(request + 1)

    override def toString: String = "plus1"
  }

  class FilterTimes2 extends Filter[Int, Int, Int, Int] {
    def apply(request: Int, service: Service[Int, Int]): Future[Int] = Future.value(request * 2)

    override def toString: String = "times2"
  }

  class FilterMinus1 extends Filter[Int, Int, Int, Int] {
    def apply(request: Int, service: Service[Int, Int]): Future[Int] = Future.value(request - 1)

    override def toString: String = "minus1"
  }

  class AgnosticFilter1 extends Filter.TypeAgnostic { self =>
    def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = {
      new Filter[Req, Rep, Req, Rep] {
        def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = ???
        override def toString: String = self.toString
      }
    }

    override def toString: String = "agnosticFilter1"
  }

  class AgnosticFilter2 extends Filter.TypeAgnostic { self =>
    def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = {
      new Filter[Req, Rep, Req, Rep] {
        def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = ???
        override def toString: String = self.toString
      }
    }
  }

  class AgnosticFilter3 extends Filter.TypeAgnostic { self =>
    def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = {
      new Filter[Req, Rep, Req, Rep] {
        def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = ???
        override def toString: String = self.toString
      }
    }

    override def toString: String = "agnosticFilter3"
  }

  class OTTypeAgnostic extends Filter.OneTime {
    def apply[Req, Rep](req: Req, svc: Service[Req, Rep]): Future[Rep] = {
      svc(req)
    }

    override def toString = "simple"
  }

  test("Filter.andThen(Filter): applies next filter") {
    val spied = spy(new PassThruFilter)
    val svc = (new PassThruFilter).andThen(spied).andThen(constSvc)
    await(svc(4))
    verify(spied).apply(any[Int], any[Service[Int, Int]])
  }

  test("Filter.andThen: toString") {
    val f1 = new FilterPlus1
    val f2 = new FilterTimes2
    val f3 = new FilterMinus1

    val af1 = new AgnosticFilter1
    val af2 = new AgnosticFilter2
    val af3 = new AgnosticFilter3

    val passThroughSvcFactory = new PassThruServiceFactory {
      override def toString: String = "passThroughSvcFactory"
    }

    // filters
    assert(f1.toString == "plus1")
    assert(f2.toString == "times2")
    assert(f3.toString == "minus1")

    // agnostic filters
    assert(af1.toString == "agnosticFilter1")
    assert(af2.toString == "com.twitter.finagle.FilterTest$AgnosticFilter2")
    assert(af3.toString == "agnosticFilter3")

    // chained filters
    val andThen = f1.andThen(f2).andThen(f3)
    assert(andThen.toString == "plus1.andThen(times2).andThen(minus1)")

    // chained filters andThen chained filters
    assert(
      andThen
        .andThen(andThen)
        .toString == "plus1.andThen(times2).andThen(minus1).andThen(plus1).andThen(times2).andThen(minus1)"
    )

    // chained agnostic filters
    val andThenAgnostic = af1.andThen(af2).andThen(af3)
    assert(
      andThenAgnostic.toString == "agnosticFilter1.andThen(com.twitter.finagle.FilterTest$AgnosticFilter2).andThen(agnosticFilter3)"
    )

    // chained agnostic filters andThen chained agnostic filters -- doesn't compose the same way as typed filters since we don't unroll in Filter.TypeAgnostic#andThen
    assert(
      andThenAgnostic
        .andThen(andThenAgnostic)
        .toString == "agnosticFilter1.andThen(com.twitter.finagle.FilterTest$AgnosticFilter2).andThen(agnosticFilter3).andThen(agnosticFilter1.andThen(com.twitter.finagle.FilterTest$AgnosticFilter2).andThen(agnosticFilter3))"
    )

    // chained filters andThen service
    val andThenSvc = andThen.andThen(constSvc)
    assert(
      andThenSvc.toString == "plus1.andThen(times2).andThen(minus1).andThen(com.twitter.finagle.service.ConstantService(ConstFuture(Return(2))))"
    )

    // chained filters andThen chained filters andThen service
    assert(
      andThen
        .andThen(andThen)
        .andThen(constSvc)
        .toString == "plus1.andThen(times2).andThen(minus1).andThen(plus1).andThen(times2).andThen(minus1).andThen(com.twitter.finagle.service.ConstantService(ConstFuture(Return(2))))"
    )

    // chained agnostic filters andThen service
    val andThenAgnosticSvc = andThenAgnostic.andThen(constSvc)
    assert(
      andThenAgnosticSvc.toString == "agnosticFilter1.andThen(com.twitter.finagle.FilterTest$AgnosticFilter2).andThen(agnosticFilter3).andThen(com.twitter.finagle.service.ConstantService(ConstFuture(Return(2))))"
    )

    // chained agnostic filters andThen chained agnostic filters andThen service -- composes like typed filters when composed with a service/serviceFactory (since it uses toFilter)
    assert(
      andThenAgnostic
        .andThen(andThenAgnostic)
        .andThen(constSvc)
        .toString == "agnosticFilter1.andThen(com.twitter.finagle.FilterTest$AgnosticFilter2).andThen(agnosticFilter3).andThen(agnosticFilter1).andThen(com.twitter.finagle.FilterTest$AgnosticFilter2).andThen(agnosticFilter3).andThen(com.twitter.finagle.service.ConstantService(ConstFuture(Return(2))))"
    )

    // chained filters andThen serviceFactory
    val andThenSvcFactory = andThen.andThen(passThroughSvcFactory)
    assert(
      andThenSvcFactory.toString == "plus1.andThen(times2).andThen(minus1).andThen(passThroughSvcFactory)"
    )

    // chained agnostic filters andThen serviceFactory
    val andThenAgnosticSvcFactory = andThenAgnostic.andThen(passThroughSvcFactory)
    assert(
      andThenAgnosticSvcFactory.toString == "agnosticFilter1.andThen(com.twitter.finagle.FilterTest$AgnosticFilter2).andThen(agnosticFilter3).andThen(passThroughSvcFactory)"
    )

    // single filter andThen service
    assert(
      f2.andThen(constSvc)
        .toString == "times2.andThen(com.twitter.finagle.service.ConstantService(ConstFuture(Return(2))))"
    )

    // single agnostic filter andThen service
    assert(
      af2
        .andThen(constSvc)
        .toString == "com.twitter.finagle.FilterTest$AgnosticFilter2.andThen(com.twitter.finagle.service.ConstantService(ConstFuture(Return(2))))"
    )

    // single filter andThen serviceFactory
    assert(f1.andThen(passThroughSvcFactory).toString == "plus1.andThen(passThroughSvcFactory)")

    // single filter andThen constant serviceFactory
    assert(
      f3.andThen(constSvcFactory)
        .toString == "minus1.andThen(com.twitter.finagle.service.ConstantService(ConstFuture(Return(2))))"
    )

    // single agnostic filter andThen serviceFactory
    assert(
      af1
        .andThen(passThroughSvcFactory)
        .toString == "agnosticFilter1.andThen(passThroughSvcFactory)"
    )

    // single agnostic filter andThen constant serviceFactory
    assert(
      af3
        .andThen(constSvcFactory)
        .toString == "agnosticFilter3.andThen(com.twitter.finagle.service.ConstantService(ConstFuture(Return(2))))"
    )

    // chained filters with Filter.identity (identity filters are excluded)
    val andThenWithIdentity =
      f1.andThen(Filter.identity[Int, Int])
        .andThen(f2)
        .andThen(Filter.identity[Int, Int])
        .andThen(f3)
        .andThen(Filter.identity[Int, Int])
    assert(andThenWithIdentity.toString == "plus1.andThen(times2).andThen(minus1)")

    // chained agnostic filters with Filter.TypeAgnostic.Identity (identity filters are excluded)
    val andThenWithAgnosticIdentity =
      af1
        .andThen(Filter.TypeAgnostic.Identity)
        .andThen(af2)
        .andThen(Filter.TypeAgnostic.Identity)
        .andThen(af3)
        .andThen(Filter.TypeAgnostic.Identity)
    assert(
      andThenWithAgnosticIdentity.toString == "agnosticFilter1.andThen(com.twitter.finagle.FilterTest$AgnosticFilter2).andThen(agnosticFilter3)"
    )

    // Filter.identity andThen filter
    assert(Filter.identity[Int, Int].andThen(f3).toString == "minus1")

    // Filter.TypeAgnostic.Identity andThen Filter.TypeAgnostic
    assert(Filter.TypeAgnostic.Identity.andThen(af3).toString == "agnosticFilter3")

    // Filter.TypeAgnostic.Identity andThen filter
    assert(Filter.TypeAgnostic.Identity.andThen(f3).toString == "minus1")

    // Filter.identity filter andThen service
    assert(
      Filter.identity
        .andThen(constSvc)
        .toString == "com.twitter.finagle.service.ConstantService(ConstFuture(Return(2)))"
    )

    // Filter.TypeAgnostic.Identity andThen service
    assert(
      Filter.TypeAgnostic.Identity
        .andThen(constSvc)
        .toString == "com.twitter.finagle.service.ConstantService(ConstFuture(Return(2)))"
    )

    // Filter.identity filter andThen serviceFactory
    assert(Filter.identity.andThen(passThroughSvcFactory).toString == "passThroughSvcFactory")

    // Filter.TypeAgnostic.Identity filter andThen serviceFactory
    assert(
      Filter.TypeAgnostic.Identity
        .andThen(passThroughSvcFactory)
        .toString == "passThroughSvcFactory"
    )

    // Filter.identity andThen Filter.identity (just collapses to one Filter.identity)
    assert(
      Filter
        .identity[Int, Int]
        .andThen(Filter.identity[Int, Int])
        .toString == "com.twitter.finagle.Filter$Identity$"
    )

    // Filter.TypeAgnostic.Identity andThen Filter.TypeAgnostic.Identity (just collapses to one Filter.identity)
    assert(
      Filter.TypeAgnostic.Identity
        .andThen(Filter.TypeAgnostic.Identity)
        .toString == "com.twitter.finagle.Filter$TypeAgnostic$Identity"
    )

    // Filter.identity andThen Filter.identity andThen filter (identity filters are excluded)
    assert(
      Filter.identity[Int, Int].andThen(Filter.identity[Int, Int]).andThen(f1).toString == "plus1"
    )

    // Filter.identity andThen Filter.identity andThen filter (identity filters are excluded)
    assert(
      Filter.TypeAgnostic.Identity
        .andThen(Filter.TypeAgnostic.Identity)
        .andThen(f1)
        .toString == "plus1"
    )

    // Filter.identity andThen Filter.identity andThen filter (identity filters are excluded)
    assert(
      Filter.identity[Int, Int].andThen(Filter.identity[Int, Int]).andThen(f1).toString == "plus1"
    )
  }

  test("Filter.andThen(Filter): lifts synchronous exceptions into Future.exception") {
    val fail = Filter.mk[Int, Int, Int, Int] { (_, _) => throw new Exception }
    val svc = (new PassThruFilter).andThen(fail).andThen(constSvc)
    val result = await(svc(4).liftToTry)
    assert(result.isThrow)
  }

  test("Filter.andThen(Service): can rescue synchronous exceptions from Service") {
    val throwSvc = Service.mk[Int, Int] { _ => throw new IllegalArgumentException("bummer") }
    val filter = new SimpleFilter[Int, Int] {
      def apply(request: Int, service: Service[Int, Int]): Future[Int] = {
        service(request).rescue {
          case _: IllegalArgumentException => Future.value(44)
        }
      }
    }

    val svc = filter.andThen(throwSvc)
    assert(44 == await(svc(4)))
  }

  test("Filter.andThen(Service): applies next filter") {
    val spied = spy(new ConstantService[Int, Int](Future.value(2)))
    val svc = (new PassThruFilter).andThen(spied)
    await(svc(4))
    verify(spied).apply(any[Int])
  }

  test("Filter.andThen(Service): applies service correctly") {
    // when applying a service to the end of a filter chain, we return a ServiceProxy,
    // ensure that the service is correctly proxied.
    val underlying = new ConstantService[Int, Int](Future.value(2))

    val counter: AtomicInteger = new AtomicInteger(0)
    val wrappedService = new ServiceProxy[Int, Int](underlying) {
      private[this] val isClosed = new AtomicBoolean(false)
      private[this] val closedP = new Promise[Unit]()

      override def apply(request: Int): Future[Int] =
        if (isClosed.get) Future.exception(new ServiceClosedException())
        else { counter.getAndIncrement(); super.apply(request) }

      override def status: Status =
        if (isClosed.get) Status.Closed
        else { counter.getAndIncrement(); underlying.status }

      override def close(deadline: Time): Future[Unit] = {
        if (isClosed.compareAndSet(false, true)) {
          counter.getAndIncrement()
          closedP.become(underlying.close(deadline))
        }
        closedP
      }
    }

    val svc = (new PassThruFilter).andThen(new PassThruFilter).andThen(wrappedService)
    await(svc(4))
    assert(counter.get() == 1)
    await(svc.close())
    assert(counter.get() == 2)
  }

  test("Filter.andThen(Service): lifts synchronous exceptions into Future.exception") {
    val svc = (new PassThruFilter).andThen(NilService)
    val result = await(svc(4).liftToTry)
    assert(result.isThrow)
  }

  test("Filter.andThen(ServiceFactory): applies next filter") {
    val spied = spy(new PassThruServiceFactory)
    val sf = (new PassThruFilter).andThen(spied)
    await(sf(ClientConnection.nil))
    verify(spied).apply(any[ClientConnection])
  }

  test("Filter.andThenIf (tuple): applies next filter when true") {
    val spied = spy(new PassThruFilter)
    val svc = (new PassThruFilter).andThenIf((true, spied)).andThen(constSvc)
    await(svc(4))
    verify(spied).apply(any[Int], any[Service[Int, Int]])
  }

  test("Filter.andThenIf (tuple): doesn't apply next filter when false") {
    val spied = spy(new PassThruFilter)
    val svc = (new PassThruFilter).andThenIf((false, spied)).andThen(constSvc)
    await(svc(4))
    verify(spied, never).apply(any[Int], any[Service[Int, Int]])
  }

  test("Filter.andThenIf (params): applies next filter when true") {
    val spied = spy(new PassThruFilter)
    val svc = (new PassThruFilter).andThenIf(true, spied).andThen(constSvc)
    await(svc(4))
    verify(spied).apply(any[Int], any[Service[Int, Int]])
  }

  test("Filter.andThenIf (params): doesn't apply next filter when false") {
    val spied = spy(new PassThruFilter)
    val svc = (new PassThruFilter).andThenIf(false, spied).andThen(constSvc)
    await(svc(4))
    verify(spied, never).apply(any[Int], any[Service[Int, Int]])
  }

  test("Filter.choose: apply the underlying filter to certain requests") {
    val spied = spy(new PassThruFilter)
    val svc = Filter
      .choose[Int, Int] {
        case req if req > 0 => spied
      }
      .andThen(constSvc)

    assert(await(svc(100)) == 2)
    verify(spied, times(1)).apply(any[Int], any[Service[Int, Int]])

    assert(await(svc(-99)) == 2)
    verify(spied, times(1)).apply(any[Int], any[Service[Int, Int]])
  }

  test("Filter.TypeAgnostic.Identity.andThen(Filter.TypeAgnostic): applies next filter") {
    val passThruFilter = new PassThruTypeAgnosticFilter

    assert(
      Filter.TypeAgnostic.Identity
        .andThen(passThruFilter)
        .getClass
        .getName == passThruFilter.getClass.getName
    )
    assert(
      passThruFilter
        .andThen(Filter.TypeAgnostic.Identity)
        .getClass
        .getName == passThruFilter.getClass.getName
    )
  }

  test("Filter.TypeAgnostic.Identity.andThen(Filter): applies next filter") {
    val passThruFilter = new PassThruFilter

    assert(
      Filter.TypeAgnostic.Identity
        .andThen(passThruFilter)
        .getClass
        .getName == passThruFilter.getClass.getName
    )
    assert(
      passThruFilter
        .agnosticAndThen(Filter.TypeAgnostic.Identity)
        .getClass
        .getName == passThruFilter.getClass.getName
    )
  }

  test("Filter.TypeAgnostic.Identity.andThen(Service): applies service") {
    assert(
      Filter.TypeAgnostic.Identity.andThen(constSvc).getClass.getName == constSvc.getClass.getName
    )
  }

  test("Filter.TypeAgnostic.Identity.andThen(ServiceFactory): applies serviceFactory") {
    val passThruServiceFactory = new PassThruServiceFactory

    assert(
      Filter.TypeAgnostic.Identity
        .andThen(passThruServiceFactory)
        .getClass
        .getName == passThruServiceFactory.getClass.getName
    )
  }

  test("Filter.TypeAgnostic.toFilter distributes over Filter.TypeAgnostic.andThen") {
    val calls = List.newBuilder[(Any, Any)]
    class TestFilter extends Filter.TypeAgnostic {
      def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] =
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
      f.andThen(Service.mk((_: Any) => Future.Done)).apply(Input)
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
        def apply(req: Req, svc: Service[Req, Rep]): Future[Rep] = svc(req)
      }

      object T extends Filter.TypeAgnostic {
        def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = new F[Req, Rep]
      }

      assert(f(T).toFilter.isInstanceOf[F[_, _]])
    }

    // Left identity
    isIdentity(Filter.TypeAgnostic.Identity.andThen)

    // Right identity
    isIdentity(_.andThen(Filter.TypeAgnostic.Identity))
  }

  test("OneTime may only generate one filter per instance") {
    val simple1, simple2, simple3 = new OTTypeAgnostic

    simple1.toFilter[Int, Int]
    intercept[IllegalStateException] {
      simple1.toFilter[Int, Int]
    }

    simple2.toFilter[Int, Int]
    intercept[IllegalStateException] {
      simple2.toFilter[Int, Int]
    }

    val invalidChain = simple3.andThen(new PassThruTypeAgnosticFilter).andThen(simple3)
    intercept[IllegalStateException] {
      invalidChain.andThen(constSvc)
    }
  }

  test("OneTime passes name for toString") {
    val simple = new OTTypeAgnostic
    assert(
      simple.andThen(new PassThruTypeAgnosticFilter).toString ==
        "simple.andThen(com.twitter.finagle.FilterTest$PassThruTypeAgnosticFilter)"
    )
  }

  test("Filter: CanStackFrom") {
    val svc: Service[Int, Int] = Service.const(Future.never)
    Stack
      .leaf(Stack.Role("svc"), svc)
      .prepend(Stack.Role("filter"), Filter.identity: Filter[Int, Int, Int, Int])

    Stack
      .leaf(Stack.Role("sf"), ServiceFactory.const(svc))
      .prepend(Stack.Role("filter"), Filter.identity: Filter[Int, Int, Int, Int])
  }

  test("Filter.TypeAgnostic: CanStackFrom") {
    val svc: Service[Int, Int] = Service.const(Future.never)
    Stack
      .leaf(Stack.Role("svc"), svc)
      .prepend(Stack.Role("filter"), Filter.TypeAgnostic.Identity)

    Stack
      .leaf(Stack.Role("sf"), ServiceFactory.const(svc))
      .prepend(Stack.Role("filter"), Filter.TypeAgnostic.Identity)
  }
}
