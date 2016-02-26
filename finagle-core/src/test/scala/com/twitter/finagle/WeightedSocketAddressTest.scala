package com.twitter.finagle

import java.net.{InetAddress, InetSocketAddress, SocketAddress}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class WeightedSocketAddressTest extends FunSuite {
  val sa = new SocketAddress {}
  val wsa = WeightedSocketAddress(sa, 1.2)
  val nestedWsa = WeightedSocketAddress(wsa, 2.2)

  test("extract: extracts address and weight") {
    val (`sa`, 1.0) = WeightedSocketAddress.extract(sa)
    val (`sa`, 1.2) = WeightedSocketAddress.extract(wsa)
    val weight = 1.2 * 2.2
    val (`sa`, `weight`) = WeightedSocketAddress.extract(nestedWsa)
  }

  test("unapply: symmetric to apply") {
    sa match {
      case _: WeightedSocketAddress => fail()
      case _ =>
    }
    val WeightedSocketAddress(`sa`, 1.2) = wsa
    val WeightedSocketAddress(`wsa`, 2.2) = nestedWsa
  }
}

@RunWith(classOf[JUnitRunner])
class WeightedInetSocketAddressTest extends FunSuite {
  val sa = new SocketAddress {}
  val wsa = WeightedSocketAddress(sa, 1.2)
  val nestedWsa = WeightedSocketAddress(wsa, 2.2)
  val ia = new InetSocketAddress(InetAddress.getLoopbackAddress, 0)
  val wia = WeightedSocketAddress(ia, 8.9)
  val nestedWia = WeightedSocketAddress(wia, 2.2)

  test("unapply: extract weights and InetSocketAddress instance") {
    val WeightedInetSocketAddress(`ia`, 8.9) = wia
    val WeightedInetSocketAddress(`ia`, 1.0) = ia
    val weight = 8.9 * 2.2
    val WeightedInetSocketAddress(`ia`, `weight`) = nestedWia
    Seq(sa, wsa, nestedWsa).foreach {
      case WeightedInetSocketAddress(_, _) => fail()
      case _ =>
    }
  }
}