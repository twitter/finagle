package com.twitter.finagle.memcached

import _root_.java.io.Serializable
import com.twitter.util.Future
import com.twitter.finagle.builder._
import com.twitter.conversions.time._
import com.twitter.finagle.memcached.protocol.text.Memcached
import _root_.java.util.{List => JList}
import scala.collection.JavaConversions._

class PooledMemcachedClientFactory(clients: Seq[SerializableKeyValueClientFactory]) extends SerializableKeyValueClientFactory {
  def this(clients: JList[SerializableKeyValueClientFactory]) = this(clients.toSeq)
  def newInstance() = new PooledMemcachedClient(clients)
}

class PooledMemcachedClient(_clients: Seq[SerializableKeyValueClientFactory]) extends KeyValueClient {
  val clients = _clients.map(_.newInstance())

  def put(key: String, value: Array[Byte]) = Future.collect(clients.map(_.put(key, value))).map(x=>())

  def release() = clients.map(_.release())
}
