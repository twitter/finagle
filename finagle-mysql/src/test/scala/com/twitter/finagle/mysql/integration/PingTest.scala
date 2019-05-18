package com.twitter.finagle.mysql.integration

import com.twitter.finagle.toggle.flag
import com.twitter.util.{Await, Duration}
import org.scalatest.FunSuite

class PingTest extends FunSuite with IntegrationClient {

  test("ping default - (toggle off)") {
    val theClient = client.orNull
    val result = Await.result(theClient.ping(), Duration.fromSeconds(1))
    // If we get here, result is Unit, and all is good
  }

}

class PingToggleOffTest extends FunSuite with IntegrationClient {

  // For this test, handshaking is done in the dispatcher.
  test("ping - (turn toggle off)") {
    flag.overrides.let("com.twitter.finagle.mysql.IncludeHandshakeInServiceAcquisition", 0.0) {
      val theClient = client.orNull
      val result = Await.result(theClient.ping(), Duration.fromSeconds(1))
      // If we get here, result is Unit, and all is good
    }
  }

}

class PingToggleOnTest extends FunSuite with IntegrationClient {

  // For this test, handshaking is done in the transporter.
  test("ping - (turn toggle on)") {
    flag.overrides.let("com.twitter.finagle.mysql.IncludeHandshakeInServiceAcquisition", 1.0) {
      val theClient = client.orNull
      val result = Await.result(theClient.ping(), Duration.fromSeconds(1))
      // If we get here, result is Unit, and all is good
    }
  }

}
