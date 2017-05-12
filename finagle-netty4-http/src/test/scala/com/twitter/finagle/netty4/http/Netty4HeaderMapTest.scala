package com.twitter.finagle.netty4.http

import com.twitter.finagle.http.{AbstractHeaderMapTest, HeaderMap => FinagleHeaders}
import io.netty.handler.codec.http.{DefaultHttpHeaders => NettyHeaders}
import scala.collection.JavaConverters._

class Netty4HeaderMapTest extends AbstractHeaderMapTest {

  override def newHeaderMap(headers: (String, String)*): FinagleHeaders = {
    val netty = new NettyHeaders()
    headers.foreach { case (k, v) => netty.add(k, v) }
    new Netty4HeaderMap(netty)
  }

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
    assert(wrapper.getOrNull("qux") == "something")
    assert(wrapper.getOrNull("missing") == null)
    assert(netty.get("qux") == "something")

    assert(wrapper.remove("foo") == Some("bar"))

    val all =
      netty.entries.asScala.map { e => e.getKey -> e.getValue }.toSet

    assert(all == Set("key" -> "val", "qux" -> "something"))

    netty.remove("key")

    assert(wrapper.toSet == Set("qux" -> "something"))
  }
}
