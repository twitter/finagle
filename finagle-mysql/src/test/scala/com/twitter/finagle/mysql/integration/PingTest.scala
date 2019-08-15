package com.twitter.finagle.mysql.integration

import com.twitter.util.{Await, Duration}
import org.scalatest.FunSuite

class PingTest extends FunSuite with IntegrationClient {

  test("ping default") {
    val theClient = client.orNull
    val result = Await.result(theClient.ping(), Duration.fromSeconds(1))
    // If we get here, result is Unit, and all is good
  }

}
