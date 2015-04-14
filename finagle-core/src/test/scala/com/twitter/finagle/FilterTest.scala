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
}
