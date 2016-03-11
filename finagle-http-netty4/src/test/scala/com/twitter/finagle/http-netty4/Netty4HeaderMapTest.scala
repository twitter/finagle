package com.twitter.finagle.http4

import com.twitter.finagle.http.{HeaderMap => FinagleHeaders}
import io.netty.handler.codec.http.{DefaultHttpHeaders => NettyHeaders}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class Netty4HeaderMapTest extends FunSuite {

  test("Netty4HeaderMap proxies updates and reads to netty headers") {
    val netty = new NettyHeaders()
    val wrapper: FinagleHeaders = new Netty4HeaderMap(netty)
    netty.add("foo", "bar")
    netty.add("key", "val")
    wrapper.add("qux", "something")

    assert(wrapper("foo") == "bar")
    assert(wrapper("key") == "val")
    assert(wrapper("qux") == "something")
    assert(wrapper.keySet == Set("foo", "key", "qux"))
    assert(wrapper.values.toSet == Set("bar", "val", "something"))
    assert(wrapper.getAll("key") == Seq("val"))
    assert(wrapper.getAll("qux") == Seq("something"))
    assert(netty.get("qux") == "something")

    assert(wrapper.remove("foo") == Some("bar"))

    val all =
      netty.entries.asScala.map { e => e.getKey -> e.getValue }.toSet

    assert(all == Set("key" -> "val", "qux" -> "something"))

    netty.remove("key")

    assert(wrapper.toSet == Set("qux" -> "something"))
  }
}
