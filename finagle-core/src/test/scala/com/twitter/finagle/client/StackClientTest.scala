package com.twitter.finagle.client

import com.twitter.finagle.param
import com.twitter.finagle.stats.InMemoryStatsReceiver
import com.twitter.util.{Await, Future, Time}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StackClientTest extends FunSuite with StringClient {
  trait Ctx {
    val sr = new InMemoryStatsReceiver
    val client = stringClient
      .configured(param.Stats(sr))
  }

  test("client stats are scoped to label") (new Ctx {
    // use dest when no label is set
    client.newService("inet!localhost:8080")
    assert(sr.counters(Seq("inet!localhost:8080", "loadbalancer", "adds")) === 1)

    // use param.Label when set
    client.configured(param.Label("myclient")).newService("localhost:8080")
    assert(sr.counters(Seq("myclient", "loadbalancer", "adds")) === 1)

    // use evaled label when both are set
    client.configured(param.Label("myclient")).newService("othername=localhost:8080")
    assert(sr.counters(Seq("othername", "loadbalancer", "adds")) === 1)
  })
}