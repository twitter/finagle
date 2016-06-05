package com.twitter.finagle.filter

import com.twitter.finagle.Service
import com.twitter.util.{Future, Promise, Return}
import org.junit.runner.RunWith
import org.mockito.Matchers.anyObject
import org.mockito.Mockito.{when, verify}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar
import scala.language.reflectiveCalls

@RunWith(classOf[JUnitRunner])
class MaskCancelFilterTest extends FunSuite with MockitoSugar {
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
