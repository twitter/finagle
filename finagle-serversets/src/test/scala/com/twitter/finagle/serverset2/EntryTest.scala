package com.twitter.finagle.serverset2

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.FunSuite

@RunWith(classOf[JUnitRunner])
class EntryTest extends FunSuite {
  val port = 80 // not bound
  val exampleJson = """{"status": "ALIVE", "additionalEndpoints": {"aurora": {"host": "10.0.0.1", "port": %d}, "http": {"host": "10.0.0.2", "port": %d}}, "serviceEndpoint": {"host": "10.0.0.3", "port": %d}, "shard": 0}""".format(port, port, port)

  test("Endpoint.parseJson: ok input") {
    val eps = Endpoint.parseJson(exampleJson)
    assert(eps.size === 3)
    val epByName = Map() ++ (eps map { ep => ep.name -> ep })
    assert(epByName.size === 3)

    assert(epByName(None) ===
      Endpoint(None, Some(HostPort("10.0.0.3", port)),
      Some(0), Endpoint.Status.Alive, ""))

    assert(epByName(Some("aurora")) ===
      Endpoint(Some("aurora"), Some(HostPort("10.0.0.1", port)),
      Some(0), Endpoint.Status.Alive, ""))

    assert(epByName(Some("http")) ===
      Endpoint(Some("http"), Some(HostPort("10.0.0.2", port)),
      Some(0), Endpoint.Status.Alive, ""))
  }

  test("Endpoint.parseJson: bad input") {
    assert(Endpoint.parseJson("hello, world!").isEmpty)
  }
}
