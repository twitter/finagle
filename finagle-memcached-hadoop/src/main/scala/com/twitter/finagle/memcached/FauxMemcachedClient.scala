package com.twitter.finagle.memcached

import _root_.java.io.Serializable
import _root_.java.util.HashMap
import com.twitter.util._
import com.twitter.finagle.builder._
import com.twitter.conversions.time._
import com.twitter.finagle.memcached.protocol.text.Memcached

class FauxMemcachedClientFactory(delay: Duration = 0.seconds) extends SerializableKeyValueClientFactory {
  def newInstance() = new FauxMemcachedClient(this, delay)
}

object FauxMemcachedClient {
  val map = new HashMap[String, Array[Byte]]
}

class FauxMemcachedClient(factory: FauxMemcachedClientFactory, delay: Duration = 0.seconds) extends KeyValueClient {
  val timer = new JavaTimer()

  def put(key: String, value: Array[Byte]) = timer.doLater(delay)(FauxMemcachedClient.map.put(key, value))

  def release() = ()
}
