package com.twitter.finagle.serverset2

import org.scalatest.FunSuite

class EntryTest extends FunSuite {
  val port = 80 // not bound
  val exampleJson =
    """{"status": "ALIVE", "additionalEndpoints": {"aurora": {"host": "10.0.0.1", "port": %d}, "http": {"host": "10.0.0.2", "port": %d}}, "serviceEndpoint": {"host": "10.0.0.3", "port": %d}, "shard": 0}"""
      .format(port, port, port)
  val exampleJson2 =
    """{"status": "ALIVE", "additionalEndpoints": {"aurora": {"host": "10.0.0.1", "port": %d}, "http": {"host": "10.0.0.1", "port": %d}}, "serviceEndpoint": {"host": "10.0.0.1", "port": %d}, "shard": 0}"""
      .format(port, port, port)

  test("Endpoint.parseJson: ok input") {
    val eps = Endpoint.parseJson(exampleJson)
    assert(eps.size == 3)
    val epByName = eps.flatMap { ep =>
      ep.names.map(_ -> ep)
    }.toMap
    assert(epByName.size == 3)

    assert(
      epByName(null) ==
        Endpoint(Array(null), "10.0.0.3", port, 0, Endpoint.Status.Alive, "")
    )

    assert(
      epByName("aurora") ==
        Endpoint(Array("aurora"), "10.0.0.1", port, 0, Endpoint.Status.Alive, "")
    )

    assert(
      epByName("http") ==
        Endpoint(Array("http"), "10.0.0.2", port, 0, Endpoint.Status.Alive, "")
    )
  }

  test("Endpoint.parseJson: ok input same hostports") {
    val eps = Endpoint.parseJson(exampleJson2)
    assert(eps.size == 1)
    val ep = eps.head
    assert(ep.names.toSet == Set(null, "aurora", "http"))
    assert(ep.host == "10.0.0.1")
    assert(ep.port == port)
    assert(ep.shard == 0)
    assert(ep.status == Endpoint.Status.Alive)
    assert(ep.memberId.isEmpty)
  }

  test("Endpoint.parseJson: bad input") {
    assert(Endpoint.parseJson("hello, world!").isEmpty)
  }
}
