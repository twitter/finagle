package com.twitter.finagle.serverset2

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class EntryTest extends FunSuite {
  val port = 80 // not bound
  val exampleJson =
    """{"status": "ALIVE", "additionalEndpoints": {"aurora": {"host": "10.0.0.1", "port": %d}, "http": {"host": "10.0.0.2", "port": %d}}, "serviceEndpoint": {"host": "10.0.0.3", "port": %d}, "shard": 0}"""
      .format(port, port, port)
  val exampleJson2 =
    """{"status": "ALIVE", "additionalEndpoints": {"aurora": {"host": "10.0.0.1", "port": %d}, "http": {"host": "10.0.0.1", "port": %d}}, "serviceEndpoint": {"host": "10.0.0.1", "port": %d}, "shard": 0}"""
      .format(port, port, port)
  val exampleCuratorJson =
    """{"name": "/example-service", "id": "00000000-0000-0000-0000-000000000000", "address": "10.0.0.1", "port": %d, "sslPort": null, "payload": null, "registrationTimeUTC": 1234567890123, "serviceType": "DYNAMIC", "uriSpec": {"parts": [{"value": "scheme", "variable": true}, {"value": "://", "variable": false}, {"value": "address", "variable": true}, {"value": ":", "variable": false}, {"value": "port", "variable": true}]}}"""
      .format(port)

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
    assert(
      eps ==
        Seq(Endpoint(Array(null, "aurora", "http"), "10.0.0.1", port, 0, Endpoint.Status.Alive, ""))
    )
  }

  test("Endpoint.parseJson: bad input") {
    assert(Endpoint.parseJson("hello, world!").isEmpty)
  }

  test("Endpoint.parseJson: ok curator input") {
    val eps = Endpoint.parseJson(exampleCuratorJson)
    assert(
      eps ==
        Seq(Endpoint(Array(null), "10.0.0.1", port, Int.MinValue, Endpoint.Status.Alive, "")))
  }
}
