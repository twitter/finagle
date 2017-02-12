package com.twitter.finagle.serverset2

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class VectorTest extends FunSuite {
  val port = 80 // not bound
  test("Selector.matches") {
    val ep1 = Endpoint(
      Array(null), "10.0.0.1", port,
      Int.MinValue, Endpoint.Status.Alive, "1234")
    val ep2 = Endpoint(
      Array(null), "1.0.0.2", port,
      3, Endpoint.Status.Alive, "12345")

    val host = Selector.Host("10.0.0.1", port)
    assert(host matches ep1)
    assert(!(host matches ep2))

    val mem = Selector.Member("12345")
    assert(!(mem matches ep1))
    assert(mem matches ep2)

    val shard = Selector.Shard(3)
    assert(!(mem matches ep1))
    assert(mem matches ep2)
  }

  test("Vector.weightOf") {
    val vec = Vector(Seq(
      Descriptor(Selector.Host("10.0.0.2", 123), 1.2, 1),
      Descriptor(Selector.Member("9876"), 1.1, 1),
      Descriptor(Selector.Member("1111"), 2.1, 1)))

    val ep1 = Endpoint(
      Array(null), "10.0.0.2", 123,
      Int.MinValue, Endpoint.Status.Alive, "1111")
    assert(vec.weightOf(ep1) == 1.2*2.1)

    val ep2 = ep1.copy(memberId="9876")
    assert(vec.weightOf(ep2) == 1.1*1.2)

    val ep3 = ep2.copy(memberId="blah")
    assert(vec.weightOf(ep3) == 1.2)

    val ep4=  ep3.copy(host = "1.1.1.1", port = 333)
    assert(vec.weightOf(ep4) == 1.0)

    for (ep <- Seq(ep1, ep2, ep3, ep4))
      assert(Vector(Seq.empty).weightOf(ep) == 1.0)
  }

  test("Vector.parseJson") {
    val Some(Vector(vec)) = Vector.parseJson("""{"vector":[{"select":"member=1","weight":1.2,"priority":1},{"select":"inet=10.0.0.3:%d","weight":1.3,"priority":2}]}""".format(port))
    assert(vec == Seq(
      Descriptor(Selector.Member("1"), 1.2, 1),
      Descriptor(Selector.Host("10.0.0.3", port), 1.3, 2)))
  }
}
