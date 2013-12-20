package com.twitter.finagle.serverset2

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite
import java.net.InetSocketAddress

@RunWith(classOf[JUnitRunner])
class ServerSet2Test extends FunSuite {
  def ia(port: Int) = new InetSocketAddress("localhost", port)
  def ep(port: Int) = Endpoint(None, ia(port), None, Endpoint.Status.Alive, port.toString)

  test("ServerSet2.weighted") {
    val ents = Set[Entry](ep(1), ep(2), ep(3), ep(4))
    val v1 = Vector(Seq(
      Descriptor(Selector.Host(ia(1)), 1.1, 1),
      Descriptor(Selector.Host(ia(2)), 1.4, 1),
      Descriptor(Selector.Member("3"), 3.1, 1)))
    val v2 = Vector(Seq(Descriptor(Selector.Member("2"), 2.0, 1)))
    val vecs = Set(v1, v2)
    
    assert(ServerSet2.weighted(ents, vecs) === Set(
      ep(1) -> 1.1,
      ep(2) -> 2.8,
      ep(3) -> 3.1,
      ep(4) -> 1.0))
  }
}
