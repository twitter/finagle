package com.twitter.finagle.pushsession

import com.twitter.finagle.pushsession.utils.{MockChannelHandle, MockPushSession}
import org.scalatest.funsuite.AnyFunSuite

class RefPushSessionTest extends AnyFunSuite {

  private type StringHandle = PushChannelHandle[String, String]
  private type StringSession = PushSession[String, String]
  private type StringRefSession = RefPushSession[String, String]

  test("Builds the initial session") {
    val handle = new MockChannelHandle[String, String]()
    val session = new MockPushSession[String, String](handle)

    val refSession = new RefPushSession(handle, session)

    refSession.receive("foo")
    assert(session.received.dequeue() == "foo")
  }

  test("Replaces the session") {
    val handle = new MockChannelHandle[String, String]()
    val session1 = new MockPushSession[String, String](handle)

    val refSession = new RefPushSession(handle, session1)

    refSession.receive("foo")
    assert(session1.received.dequeue() == "foo")

    val session2 = new MockPushSession[String, String](handle)
    refSession.updateRef(session2)
    refSession.receive("bar")
    val closeF = refSession.close()

    assert(session1.received.isEmpty)
    assert(!session1.closeCalled)

    assert(session2.received.dequeue() == "bar")

    // the close call hasn't been handled by the executor yet
    assert(!session2.closeCalled)
    assert(!closeF.isDefined)

    // now the close call has been handled by the executor
    handle.serialExecutor.executeAll()
    assert(session2.closeCalled)
    assert(closeF.isDefined)
  }
}
