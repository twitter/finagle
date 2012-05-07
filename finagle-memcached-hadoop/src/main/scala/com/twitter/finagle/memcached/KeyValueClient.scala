package com.twitter.finagle.memcached

import _root_.java.io.Serializable
import com.twitter.util.Future
import com.twitter.finagle.builder._
import com.twitter.conversions.time._
import com.twitter.finagle.memcached.protocol.text.Memcached

trait SerializableKeyValueClientFactory extends Serializable {
  def newInstance(): KeyValueClient
}

trait KeyValueClient {
  def put(key: String, value: Array[Byte]): Future[Unit]
  def release(): Unit
}
