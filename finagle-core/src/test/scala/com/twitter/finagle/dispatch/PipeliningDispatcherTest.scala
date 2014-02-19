package com.twitter.finagle.dispatch

import com.twitter.util.Future
import com.twitter.finagle.transport.Transport
import org.junit.runner.RunWith
import org.mockito.Mockito.{when, never, verify}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class PipeliningDispatcherTest extends FunSuite with MockitoSugar {
  test("PipeliningDispatcher: should treat interrupts properly") {
    val trans = mock[Transport[Unit, Unit]]
    when(trans.write(())).thenReturn(Future.Done)
    when(trans.read()).thenReturn(Future.never)
    val dispatch = new PipeliningDispatcher[Unit, Unit](trans)
    val f = dispatch(())
    f.raise(new Exception())
    verify(trans, never()).close()
  }
}
