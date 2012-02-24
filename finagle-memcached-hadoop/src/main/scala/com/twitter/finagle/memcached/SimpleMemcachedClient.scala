package com.twitter.finagle.memcached

import _root_.java.io.Serializable
import com.twitter.util.Future
import com.twitter.finagle.builder._
import com.twitter.conversions.time._
import com.twitter.finagle.memcached.protocol.text.Memcached

class SimpleMemcachedClientFactory(hosts: Seq[String], port: Int, hostConnectionLimit: Int = 10) {
  def newInstance() = new SimpleMemcachedClient(hosts, port, hostConnectionLimit)
}

class SimpleMemcachedClient(hosts: Seq[String], port: Int, hostConnectionLimit: Int = 10) extends KeyValueClient {
  val forever = Int.MaxValue.seconds.fromNow
  val mkhosts = hosts.map{ host => host + ":" + String.valueOf(port) }.mkString(",")
  val client = Client(
    ClientBuilder()
      .hosts(mkhosts)
      .hostConnectionLimit(hostConnectionLimit)
      .codec(new Memcached)
      .build()).withBytes

  def put(key: String, value: Array[Byte]) = {
    client.set(key, 0, forever, value)
  }
  
  def release() = {
    client.release()
  }
}