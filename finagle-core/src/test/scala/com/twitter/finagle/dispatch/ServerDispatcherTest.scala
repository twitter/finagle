package com.twitter.finagle.dispatch

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.junit.runner.RunWith
import org.scalatest.mock.MockitoSugar
import org.mockito.Mockito.{times, verify, when}
import org.mockito.Matchers._
import com.twitter.finagle.transport.Transport
import com.twitter.util.{Promise, Time, Future}
import com.twitter.finagle.Service

@RunWith(classOf[JUnitRunner])
class ServerDispatcherTest extends FunSuite with MockitoSugar {
  test("dispatch one at a time") {
    val trans = mock[Transport[String, String]]
    when(trans.onClose) thenReturn  Future.never
    val service = mock[Service[String, String]]
    when(service.close(any[Time])) thenReturn  Future.Done

    val readp = new Promise[String]
    when(trans.read()) thenReturn readp

    val disp = new SerialServerDispatcher(trans, service)
    verify(trans).read()
    verify(trans, times(0)).write(any[String])
    verify(service, times(0))(any[String])

    val servicep = new Promise[String]
    when(service(any[String])) thenReturn servicep

    readp.setValue("ok")
    verify(service)("ok")
    verify(trans, times(0)).write(any[String])

    val writep = new Promise[Unit]
    when(trans.write(any[String])) thenReturn writep

    servicep.setValue("ack")
    verify(trans).write("ack")

    verify(trans).read()
    when(trans.read()) thenReturn new Promise[String] // to short circuit
    writep.setDone()
    verify(trans, times(2)).read()

  }

  class helper {
    val onClose = new Promise[Throwable]
    val writep = new Promise[Unit]
    val trans = mock[Transport[String, String]]
    when(trans.onClose) thenReturn onClose
    when(trans.write(any[String])) thenReturn writep
    val service = mock[Service[String, String]]
    when(service.close(any[Time])) thenReturn Future.Done
    val replyp = new Promise[String] {
      @volatile var interrupted: Option[Throwable] = None
      setInterruptHandler { case exc => interrupted = Some(exc) }
    }
    when(service("ok")) thenReturn replyp

    val readp = new Promise[String]
    when(trans.read()) thenReturn  readp

    val disp = new SerialServerDispatcher(trans, service)
  }

  test("interrupt on hangup while pending") {
    val h = new helper
    import h._

    readp.setValue("ok")
    verify(service).apply("ok")
    assert(replyp.interrupted == None)
    onClose.setValue(new Exception)
    assert(replyp.interrupted match{
      case Some(any) => true
    })
  }

  test("interrupt while reading"){
    val h = new helper
    import h._

    readp.setValue("ok")
    verify(service)("ok")
    replyp.setValue("yes")
    disp.close(Time.now)
    assert(replyp.interrupted == None)
    verify(trans).write("yes")
    onClose.setValue(new Exception)
    assert(replyp.interrupted == None)
  }

  test("interrupt while draining"){
    val h = new helper
    import h._

    readp.setValue("ok")
    verify(service)("ok")
    replyp.setValue("yes")
    disp.close(Time.now)
    assert(replyp.interrupted == None)
    verify(trans).write("yes")
    onClose.setValue(new Exception)
    assert(replyp.interrupted == None)
  }

  class helper2{
    val onClose = new Promise[Throwable]
    val writep = new Promise[Unit]
    val trans = mock[Transport[String, String]]
    when(trans.onClose) thenReturn  onClose
    when(trans.write(any[String])) thenReturn  writep

    val service = mock[Service[String, String]]
    when(service.close(any[Time])) thenReturn  Future.Done

    val readp = new Promise[String]
    when(trans.read()) thenReturn  readp

    val disp = new SerialServerDispatcher(trans, service)
    verify(trans).read()
  }

  test("drain while reading") {
    val h = new helper2
    import h._

    disp.close(Time.now)
    verify(trans).close(any[Time])
    verify(service, times(0)).close(any[Time])

    readp.setException(new Exception("closed!"))
    onClose.setValue(new Exception("closed!"))
    verify(service).close(any[Time])
    verify(service, times(0)).apply(any[String])
    verify(trans, times(0)).write(any[String])
    verify(trans).read()
  }

  test("drain while dispatching"){
    val h = new helper2
    import h._

    val servicep = new Promise[String]
    when(service(any[String])) thenReturn servicep
    readp.setValue("ok")
    verify(service)("ok")

    disp.close(Time.now)
    verify(service, times(0)).close(any[Time])
    verify(trans, times(0)).close()

    servicep.setValue("yes")
    verify(trans).write("yes")
    verify(service, times(0)).close(any[Time])
    verify(trans, times(0)).close()

    writep.setDone()
    verify(trans).close()
    onClose.setValue(new Exception("closed!"))
    verify(service).close(any[Time])
  }
}
