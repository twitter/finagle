package com.twitter.finagle.postgres.connection

import com.twitter.finagle.postgres.Spec
import com.twitter.finagle.postgres.messages.{Query, ParameterStatus, NotificationResponse, NoticeResponse}

class ConnectionAsyncSpec extends Spec {
  "A postgres connection" should {
    "ignore async messages for new connection" in {
      val connection = new Connection()

      val response = connection.receive(NotificationResponse(-1, "", ""))
      response must equal(None)

      val response2 = connection.receive(NoticeResponse(Some("blahblah")))
      response2 must equal(None)

      val response3 = connection.receive(ParameterStatus("foo", "bar"))
      response3 must equal(None)
    }

    "ignore async messages for connected client" in {
      val connection = new Connection(Connected)

      val response = connection.receive(NotificationResponse(-1, "", ""))
      response must equal(None)

      val response2 = connection.receive(NoticeResponse(Some("blahblah")))
      response2 must equal(None)

      val response3 = connection.receive(ParameterStatus("foo", "bar"))
      response3 must equal(None)
    }

    "ignore async messages when in query" in {
      val connection = new Connection(Connected)

      connection.send(Query("select * from Test"))

      val response = connection.receive(NotificationResponse(-1, "", ""))
      response must equal(None)

      val response2 = connection.receive(NoticeResponse(Some("blahblah")))
      response2 must equal(None)

      val response3 = connection.receive(ParameterStatus("foo", "bar"))
      response3 must equal(None)
    }
  }
}
