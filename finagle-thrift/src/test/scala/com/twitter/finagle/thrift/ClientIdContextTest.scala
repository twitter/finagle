package com.twitter.finagle.thrift

import com.twitter.io.Buf
import org.junit.runner.RunWith
import org.scalatest.{OneInstancePerTest, BeforeAndAfter, FunSuite}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ClientIdContextTest extends FunSuite
  with BeforeAndAfter
  with OneInstancePerTest
{

  before { ClientId.clear() }
  after { ClientId.clear() }

  val clientId = ClientId("derpbird.staging")
  val handler = new ClientIdContext

  test("Emit/handle a set ClientId") {
    ClientId.set(Some(clientId))
    val Some(buf) = handler.emit()
    ClientId.clear()
    handler.handle(buf)

    assert(ClientId.current === Some(clientId))
  }

  test("Emit/handle a None ClientId") {
    ClientId.set(None)
    assert(handler.emit() === None)

    ClientId.clear()
    handler.handle(Buf.Empty)
    assert(ClientId.current === None)
  }
}
