package com.twitter.finagle.factory

import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.util.{Await, Future, Promise, MockTimer, Return, Time}
import org.mockito.Matchers.any
import org.mockito.Mockito.{verify, when}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.language.reflectiveCalls

@RunWith(classOf[JUnitRunner])
class TimeoutFactoryTest extends FunSuite with MockitoSugar {

  trait TimeoutFactoryHelper {
    val timer = new MockTimer
    val underlying = mock[ServiceFactory[String, String]]
    when(underlying.close(any[Time])).thenReturn(Future.Done)
    val promise = new Promise[Service[String, String]] {
      @volatile var interrupted: Option[Throwable] = None
      setInterruptHandler { case exc => interrupted = Some(exc) }
    }
    when(underlying(any[ClientConnection])).thenReturn(promise)
    val timeout = 1.second
    val exception = new ServiceTimeoutException(timeout)
    val factory = new TimeoutFactory(underlying, 1.second, exception, timer)
  }

  trait AfterHelper extends TimeoutFactoryHelper {
    val res = factory()
    Time.withCurrentTimeFrozen { tc =>
      verify(underlying)(any[ClientConnection])
      assert(promise.interrupted == None)
      assert(!res.isDefined)
      tc.advance(5.seconds)
      timer.tick()
    }
  }

  test("TimeoutFactory after the timeout should fail the service acquisition") {
    new AfterHelper {
      assert(res.isDefined)
      val failure = intercept[Failure] {
        Await.result(res)
      }
      assert(failure.getCause.isInstanceOf[TimeoutException])
      assert(failure.getCause == exception)
    }
  }

  test("TimeoutFactory after the timeout should interrupt the underlying promise with a TimeoutException") {
    new AfterHelper {
      assert(promise.interrupted forall {
        case _: java.util.concurrent.TimeoutException => true
        case _ => false
      })
    }
  }

  test("TimeoutFactory before the timeout should pass the successfully created service through") {
    new TimeoutFactoryHelper {
      val res = factory()
      assert(!res.isDefined)
      val service = mock[Service[String, String]]
      when(service.close(any[Time])).thenReturn(Future.Done)
      promise() = Return(service)
      assert(res.isDefined)
      assert(res.poll == Some(Return(service)))
    }
  }
}
