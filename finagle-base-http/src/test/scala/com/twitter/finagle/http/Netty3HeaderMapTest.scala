package com.twitter.finagle.http

import org.jboss.netty.handler.codec.http.DefaultHttpHeaders
import scala.collection.JavaConverters._

class Netty3HeaderMapTest extends AbstractHeaderMapTest {

  def newHeaderMap(headers: (String, String)*): HeaderMap = {
    val result = Request().headerMap
    headers.foreach { case (k, v) => result.add(k, v) }
    result
  }

  test("Netty3HeaderMap proxies updates and reads to netty headers") {
    val netty = new DefaultHttpHeaders()
    val wrapper: HeaderMap = new Netty3HeaderMap(netty)
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
