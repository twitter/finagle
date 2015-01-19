package com.twitter.finagle.memcachedx.unit

import com.twitter.concurrent.Broker
import com.twitter.conversions.time._
import com.twitter.finagle._
import com.twitter.finagle.memcachedx._
import com.twitter.util.{Command => _, Function => _, _}
import org.junit.runner.RunWith
import org.mockito.Mockito.{times, verify, when}
import org.mockito.Matchers
import org.mockito.Matchers._
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class KetamaFailureAccrualFactoryTest extends FunSuite with MockitoSugar {

  class Helper(
      ejectFailedHost: Boolean,
      serviceRep: Future[Int] = Future.exception(new Exception))
  {
    val underlyingService = mock[Service[Int, Int]]
    when(underlyingService.close(any[Time])) thenReturn Future.Done
    when(underlyingService.status) thenReturn Status.Open
    when(underlyingService(Matchers.anyInt)) thenReturn serviceRep

    val underlying = mock[ServiceFactory[Int, Int]]
    when(underlying.close(any[Time])) thenReturn Future.Done
    when(underlying.status) thenReturn Status.Open
    when(underlying()) thenReturn Future.value(underlyingService)

    val key = mock[KetamaClientKey]
    val broker = new Broker[NodeHealth]

    val timer = new MockTimer
    val factory =
      new KetamaFailureAccrualFactory[Int, Int](
        underlying, 3, 10.seconds, timer, key, broker, ejectFailedHost)

    val service = Await.result(factory())
    verify(underlying)()
  }

  test("fails immediately after consecutive failures, revive after markDeadFor duration") {
    val h = new Helper(false)
    import h._

    Time.withCurrentTimeFrozen { timeControl =>
      intercept[Exception] {
        Await.result(service(123))
      }
      intercept[Exception] {
        Await.result(service(123))
      }
      assert(factory.isAvailable)
      assert(service.isAvailable)

      // triggers markDead
      intercept[Exception] {
        Await.result(service(123))
      }
      assert(!factory.isAvailable)
      assert(!service.isAvailable)
      assert(broker.recv.sync().isDefined === false)

      // skips dispatch
      intercept[FailureAccrualException] {
        Await.result(factory())
      }
      verify(underlyingService, times(3))(123)

      timeControl.advance(10.seconds)
      timer.tick()

      // revives after duration
      assert(factory.isAvailable)
      assert(service.isAvailable)
      assert(broker.recv.sync().isDefined === false)

      when(underlyingService(123)) thenReturn Future.value(123)

      assert(Await.result(service(123)) === 123)

      // failures # is reset to 0
      intercept[Exception] {
        Await.result(service(456))
      }
      assert(factory.isAvailable)
      assert(service.isAvailable)
      verify(underlyingService, times(4))(123)
      verify(underlyingService, times(1))(456)
    }
  }

  test("eject and revive failed host when ejectFailedHost=true") {
    val h = new Helper(true)
    import h._

    Time.withCurrentTimeFrozen { timeControl =>
      intercept[Exception] {
        Await.result(service(123))
      }
      intercept[Exception] {
        Await.result(service(123))
      }
      assert(factory.isAvailable)
      assert(service.isAvailable)

      // triggers markDead
      intercept[Exception] {
        Await.result(service(123))
      }
      assert(!factory.isAvailable)
      assert(!service.isAvailable)

      // ejects
      val recv = broker.recv.sync()
      assert(Await.result(recv) === NodeMarkedDead(key))

      timeControl.advance(10.seconds)
      timer.tick()

      // revives after duration
      assert(factory.isAvailable)
      assert(service.isAvailable)

      val recv2 = broker.recv.sync()
      assert(Await.result(recv2) === NodeRevived(key))
    }
  }

  test("treats successful response and cancelled exceptions as success") {
    val successes =
      Seq(
        Future.value(123),
        Future.exception(new CancelledRequestException(new Exception)),
        Future.exception(new CancelledConnectionException(new Exception)),
        Future.exception(ChannelWriteException(new CancelledRequestException(new Exception))),
        Future.exception(ChannelWriteException(new CancelledConnectionException(new Exception))))

    successes.foreach { rep =>
      val h = new Helper(false, rep)
      import h._

      def assertReponse(rep: Future[Int]) {
        if (rep.isReturn) assert(Await.result(service(123)) === rep.get)
        else intercept[Exception](Await.result(service(123)))
      }

      Time.withCurrentTimeFrozen { _ =>
        assertReponse(rep)
        assertReponse(rep)
        assert(factory.isAvailable)
        assert(service.isAvailable)

        // not trigger markDead
        assertReponse(rep)
        assert(factory.isAvailable)
        assert(service.isAvailable)
      }
    }
  }
}