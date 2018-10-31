package com.twitter.finagle.pushsession.utils

import com.twitter.finagle.Status
import com.twitter.finagle.pushsession.{PushChannelHandle, PushSession}
import com.twitter.util.{Future, Time}
import scala.collection.mutable

class MockPushSession[In, Out](handle: PushChannelHandle[In, Out])
    extends PushSession[In, Out](handle) {

  val received: mutable.Queue[In] = mutable.Queue.empty[In]

  var closeCalled: Boolean = false

  def receive(message: In): Unit = received += message

  def status: Status = handle.status

  def close(deadline: Time): Future[Unit] = {
    closeCalled = true
    Future.Unit
  }
}
