package com.twitter.finagle.http2.transport.client

import com.twitter.finagle.FailureFlags
import com.twitter.finagle.Service
import com.twitter.finagle.Status
import com.twitter.util.Await
import com.twitter.util.Awaitable
import com.twitter.util.Duration
import com.twitter.util.Future
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.OneInstancePerTest
import org.scalatestplus.mockito.MockitoSugar
import org.scalatest.funsuite.AnyFunSuite

class RefCountedFactoryTest extends AnyFunSuite with MockitoSugar with OneInstancePerTest {

  private[this] def await[T](t: Awaitable[T]): T = Await.result(t, Duration.fromSeconds(2))

  private[this] val service = mock[Service[String, String]]
  when(service.close()).thenReturn(Future.Unit)

  private[this] val factory = new RefCountedFactory[String, String](service)

  test("status") {
    Seq(Status.Open, Status.Busy, Status.Closed).foreach { status =>
      when(service.status).thenReturn(status)
      assert(factory.status == status)
    }
  }

  test("status while closing but not drained") {
    when(service.status).thenReturn(Status.Open)
    val s = await(factory())
    assert(s.status == Status.Open)
    assert(factory.status == Status.Open)
    // close the factory but not the service
    await(factory.close())
    assert(factory.status == Status.Closed)

    // The checked out service should reflect the underlying regardless of the factory
    // TODO: is this the behavior we want? Maybe we want to signal Closed to drain?
    assert(s.status == Status.Open)
  }

  test("checkout 1") {
    when(service.apply(any(classOf[String]))).thenReturn(Future.value("bar"))
    val s = await(factory.apply().flatMap(_("foo")))
    assert(s == "bar")
  }

  test("checkout after closed") {
    await(factory.close())
    verify(service, times(1)).close()

    val ex = intercept[FailureFlags[_]] {
      await(factory())
    }

    assert(ex.isFlagged(FailureFlags.Retryable))
  }

  test("checkout after draining") {
    val s = await(factory())
    await(factory.close())
    verify(service, times(0)).close()

    val ex = intercept[FailureFlags[_]] {
      await(factory())
    }

    assert(ex.isFlagged(FailureFlags.Retryable))
    await(s.close())
    verify(service, times(1)).close()
  }

  test("close while nothing is checked out") {
    await(factory.close())
    verify(service, times(1)).close()
    // Do it again, but we shouldn't re-close the service.
    await(factory.close())
    verify(service, times(1)).close()
  }

  test("close while session checked out") {
    val s = await(factory())
    await(factory.close())

    verify(service, times(0)).close()

    // now close the checked out service
    await(s.close())
    verify(service, times(1)).close()
  }

  test("multiple factory closed while checked out") {
    val s = await(factory())
    await(factory.close())
    await(factory.close())

    verify(service, times(0)).close()

    // now close the checked out service
    await(s.close())
    verify(service, times(1)).close()
  }

  test("multiple service close") {
    val s1 = await(factory())
    val s2 = await(factory())
    await(factory.close())
    await(factory.close())

    verify(service, times(0)).close()

    // now close the checked out service
    await(s1.close())
    verify(service, times(0)).close()
    // Still nothing, we already spent this wrappers close
    await(s1.close())
    verify(service, times(0)).close()

    // finally close the last remaining checked out service
    await(s2.close())
    verify(service, times(1)).close()
  }

}
