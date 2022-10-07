package com.twitter.finagle.filter

import com.twitter.finagle.Service
import com.twitter.util.Future
import com.twitter.util.Promise
import com.twitter.util.Return
import org.mockito.ArgumentMatchers.anyObject
import org.mockito.Mockito.when
import org.mockito.Mockito.verify
import org.scalatestplus.mockito.MockitoSugar
import scala.language.reflectiveCalls
import org.scalatest.funsuite.AnyFunSuite

class MaskCancelFilterTest extends AnyFunSuite with MockitoSugar {
  trait MaskHelper {
    val service = mock[Service[Int, Int]]
    when(service.close(anyObject)).thenReturn(Future.Done)
    val filter = new MaskCancelFilter[Int, Int]

    val filtered = filter andThen service
    val p = new Promise[Int] {
      @volatile var interrupted: Option[Throwable] = None
      setInterruptHandler { case exc => interrupted = Some(exc) }
    }
    when(service(1)).thenReturn(p)

    val f = filtered(1)
    verify(service).apply(1)
  }

  test("MaskCancelFilter should mask interrupts") {
    new MaskHelper {
      assert(p.interrupted == None)
      f.raise(new Exception)
      assert(p.interrupted == None)
    }
  }

  test("MaskCancelFilter should propagate results") {
    new MaskHelper {
      assert(f.poll == None)
      p.setValue(123)
      assert(p.poll == Some(Return(123)))
    }
  }
}
