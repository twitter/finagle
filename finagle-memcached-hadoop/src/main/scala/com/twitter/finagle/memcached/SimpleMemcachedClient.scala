package com.twitter.finagle.memcached

import _root_.java.io.Serializable
import com.twitter.util.Future
import com.twitter.finagle.builder._
import com.twitter.conversions.time._
import com.twitter.finagle.memcached.protocol.text.Memcached
import scala.collection.JavaConversions._
import _root_.java.util.{List => JList}

class SimpleMemcachedClientFactory(hosts: Seq[String], port: Int) extends SerializableKeyValueClientFactory {
  def this(hosts: JList[String], port: Int) = this(hosts.toSeq, port)
  def newInstance() = new SimpleMemcachedClient(hosts, port)
}

class SimpleMemcachedClient(hosts: Seq[String], port: Int) extends KeyValueClient {
  val forever = Int.MaxValue.seconds.fromNow
  val mkhosts = hosts.map{ host => host + ":" + String.valueOf(port) }.mkString(",")
  val client = KetamaClientBuilder()
          .nodes(mkhosts)
          .build().withBytes

  def put(key: String, value: Array[Byte]) = {
    client.set(key, 0, forever, value)
  }

  def release() = {
    client.release()
  }
}
