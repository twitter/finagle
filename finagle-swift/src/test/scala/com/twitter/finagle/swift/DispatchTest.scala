package com.twitter.finagle.exp.swift

import com.twitter.util.{Await, Future}
import org.apache.thrift.TApplicationException
import org.apache.thrift.protocol._
import org.apache.thrift.transport._
import org.junit.runner.RunWith
import org.mockito.Mockito.{verify, when}
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.scalatest.mock.MockitoSugar

@RunWith(classOf[JUnitRunner])
class DispatchTest extends FunSuite with MockitoSugar {
  import Util._

  test("dispatches to the right method") {
    val processor = mock[Test1]
    val service = new SwiftService(processor)
    
    val message = newMessage("ping", TMessageType.CALL) { out =>
      out.writeFieldBegin(new TField("msg", TType.STRING, 1))
      out.writeString("HELLO")
    }

    when(processor.ping("HELLO")).thenReturn(Future.never)
    service(message)
    verify(processor).ping("HELLO")
  }

  test("fails when dispatching to nonexisting method") {
    val processor = mock[Test1]
    val service = new SwiftService(processor)
    val message = newMessage("pong", TMessageType.CALL) { out => () }

    val f = service(message)
    val exc = intercept[TApplicationException] { Await.result(f) }
    assert(exc.getType() === TApplicationException.UNKNOWN_METHOD)
    assert(exc.getMessage() === "Unknown method pong")
  }
}
