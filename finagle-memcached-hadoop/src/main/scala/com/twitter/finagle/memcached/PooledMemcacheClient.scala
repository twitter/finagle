package com.twitter.finagle.memcached

import _root_.java.io.Serializable
import com.twitter.util.Future
import com.twitter.finagle.builder._
import com.twitter.conversions.time._
import com.twitter.finagle.memcached.protocol.text.Memcached

class PooledMemcachedClientFactory(pools: Seq[SerializableKeyValueClientFactory]) {
  def newInstance() = new PooledMemcachedClient(pools)
}

class PooledMemcachedClient(pools: Seq[SerializableKeyValueClientFactory]) extends KeyValueClient {
  val clients = pools.map(_.newInstance())

  def put(key: String, value: Array[Byte]) = Future.collect(clients.map(_.put(key, value))).map(x=>())

  def release() = clients.map(_.release())
}
