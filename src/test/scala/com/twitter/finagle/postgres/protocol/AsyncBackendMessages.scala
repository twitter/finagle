package com.twitter.finagle.postgres.protocol

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import org.specs2.mutable.Specification

@RunWith(classOf[JUnitRunner])
class AsyncBackendMessages extends Specification with ConnectionSpec {

  "Postgres client" should {
    "ignore async messages for new connection" inConnection {
      implicit var connection = new ConnectionStateMachine()
      receive(NotificationResponse(-1, "", ""))
      response === None
      receive(NoticeResponse(Some("blahblah")))
      response === None
      receive(ParameterStatus("foo", "bar"))
      response === None
    }

    "ignore async messages for connected client" inConnection {
      implicit var connection = new ConnectionStateMachine(state = Connected)
      receive(NotificationResponse(-1, "", ""))
      response === None
      receive(NoticeResponse(Some("blahblah")))
      response === None
      receive(ParameterStatus("foo", "bar"))
      response === None
    }

    "ignore async messages when in query" inConnection {
      implicit var connection = new ConnectionStateMachine(state = Connected)

      send(Query("select * from Test"))

      receive(NotificationResponse(-1, "", ""))
      response === None
      receive(NoticeResponse(Some("blahblah")))
      response === None
      receive(ParameterStatus("foo", "bar"))
      response === None
    }
  }

}
