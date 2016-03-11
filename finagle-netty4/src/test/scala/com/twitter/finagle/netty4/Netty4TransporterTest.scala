package com.twitter.finagle.netty4

import com.twitter.finagle.Stack.Params
import com.twitter.finagle.util.InetSocketAddressUtil
import com.twitter.util.{Duration, Await}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Netty4TransporterTest extends FunSuite with Eventually with IntegrationPatience {

  test("connection failures are propagated to the transporter promise") {
    val transporter = Netty4Transporter(None, None, Params.empty)

    val p = transporter(InetSocketAddressUtil.unconnected)

    // connection failure is propagated to the Transporter promise
    intercept[java.nio.channels.UnsupportedAddressTypeException] {
      Await.result(p, Duration.fromSeconds(15))
    }
  }
}