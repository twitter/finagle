package com.twitter.finagle.serverset2

import com.twitter.util.RandomSocket
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import java.net.InetSocketAddress

@RunWith(classOf[JUnitRunner])
class ServerSet2Test extends FunSuite {
  def ia(port: Int) = new InetSocketAddress("localhost", port)
  def ep(port: Int) = Endpoint(None, ia(port), None, Endpoint.Status.Alive, port.toString)

  test("ServerSet2.weighted") {
    val port1 = RandomSocket.nextPort()
    val port2 = RandomSocket.nextPort()
    val ents = Set[Entry](ep(port1), ep(port2), ep(3), ep(4))
    val v1 = Vector(Seq(
      Descriptor(Selector.Host(ia(port1)), 1.1, 1),
      Descriptor(Selector.Host(ia(port2)), 1.4, 1),
      Descriptor(Selector.Member("3"), 3.1, 1)))
    val v2 = Vector(Seq(Descriptor(Selector.Member(port2.toString), 2.0, 1)))
    val vecs = Set(v1, v2)

    assert(ServerSet2.weighted(ents, vecs) === Set(
      ep(port1) -> 1.1,
      ep(port2) -> 2.8,
      ep(3) -> 3.1,
      ep(4) -> 1.0))
  }
}
