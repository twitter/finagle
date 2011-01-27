package com.twitter.finagle.memcached

import _root_.java.util.TreeMap
import com.twitter.finagle.memcached.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.buffer.ChannelBuffer
import scala.collection.JavaConversions._
import com.twitter.finagle.builder.ClientBuilder
import text.Memcached
import com.twitter.finagle.Service
import com.twitter.util.{Time, Future}

object Client {
  def apply(host: String): Client = Client(
    ClientBuilder()
      .hosts(host)
      .codec(new Memcached)
      .build())

  def apply(services: Seq[Service[Command, Response]]): Client = {
    new PartitionedClient(services.map(apply(_)), _.hashCode)
  }

  def apply(raw: Service[Command, Response]): Client = {
    new ConnectedClient(raw)
  }
}

trait Client {
  def set(key: String, flags: Int, expiry: Time, value: ChannelBuffer):     Future[Response]
  def add(key: String, flags: Int, expiry: Time, value: ChannelBuffer):     Future[Response]
  def append(key: String, flags: Int, expiry: Time, value: ChannelBuffer):  Future[Response]
  def prepend(key: String, flags: Int, expiry: Time, value: ChannelBuffer): Future[Response]
  def replace(key: String, flags: Int, expiry: Time, value: ChannelBuffer): Future[Response]

  def get(key: String):                           Future[Option[ChannelBuffer]]
  def get(keys: Iterable[String]):                Future[Map[String, ChannelBuffer]]
  def delete(key: String):                        Future[Response]
  def incr(key: String):                          Future[Int]
  def incr(key: String, delta: Int):              Future[Int]
  def decr(key: String):                          Future[Int]
  def decr(key: String, delta: Int):              Future[Int]

  def set(key: String, value: ChannelBuffer):     Future[Response] = set(key, 0, Time.epoch, value)
  def add(key: String, value: ChannelBuffer):     Future[Response] = add(key, 0, Time.epoch, value)
  def append(key: String, value: ChannelBuffer):  Future[Response] = append(key, 0, Time.epoch, value)
  def prepend(key: String, value: ChannelBuffer): Future[Response] = prepend(key, 0, Time.epoch, value)
  def replace(key: String, value: ChannelBuffer): Future[Response] = replace(key, 0, Time.epoch, value)
}

protected class ConnectedClient(underlying: Service[Command, Response]) extends Client {
  def get(key: String) = {
    underlying(Get(Seq(key))) map {
      case Values(values) =>
        if (values.size > 0) Some(values.head.value)
        else None
    }
  }

  def get(keys: Iterable[String]) = {
    underlying(Get(keys.toSeq)) map {
      case Values(values) =>
        val tuples = values.map {
          case Value(key, value) =>
            (key.toString(CharsetUtil.UTF_8), value)
        }
        Map(tuples: _*)
    }
  }

  def set(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    underlying(Set(key, flags, expiry, value))
  def add(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    underlying(Add(key, flags, expiry, value))
  def append(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    underlying(Append(key, flags, expiry, value))
  def prepend(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    underlying(Prepend(key, flags, expiry, value))
  def replace(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    underlying(Replace(key, flags, expiry, value))

  def delete(key: String)                        = underlying(Delete(key))
  def incr(key: String): Future[Int]             = incr(key, 1)
  def decr(key: String): Future[Int]             = decr(key, 1)

  def incr(key: String, delta: Int): Future[Int] = {
    underlying(Incr(key, delta)) map {
      case Number(value) =>
        value
    }
  }


  def decr(key: String, delta: Int): Future[Int] = {
    underlying(Decr(key, delta)) map {
      case Number(value) =>
        value
    }
  }

  override def toString = hashCode.toString // FIXME this incompatible with Ketama
}

class PartitionedClient(clients: Seq[Client], hash: String => Long) extends Client {
  require(clients.size > 0, "At least one client must be provided")

  private[this] val circle = {
    val circle = new TreeMap[Long, Client]()
    clients foreach { client =>
      circle += hash(client.toString) -> client
    }
    circle
  }

  def get(key: String)                    = idx(key).get(key)
  def get(keys: Iterable[String])         = {
    val keysGroupedByClient = keys.groupBy(idx(_))

    val mapOfMaps = keysGroupedByClient.map { case (client, keys) =>
      client.get(keys)
    }

    mapOfMaps.reduceLeft { (result, nextMap) =>
      for {
        result <- result
        nextMap <- nextMap
      } yield {
        result ++ nextMap
      }
    }
  }

  def set(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    idx(key).set(key, flags, expiry, value)
  def add(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    idx(key).add(key, flags, expiry, value)
  def append(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    idx(key).append(key, flags, expiry, value)
  def prepend(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    idx(key).prepend(key, flags, expiry, value)
  def replace(key: String, flags: Int, expiry: Time, value: ChannelBuffer) =
    idx(key).replace(key, flags, expiry, value)

  def delete(key: String)                        = idx(key).delete(key)
  def incr(key: String)                          = idx(key).incr(key)
  def incr(key: String, delta: Int)              = idx(key).incr(key, delta)
  def decr(key: String)                          = idx(key).decr(key)
  def decr(key: String, delta: Int)              = idx(key).decr(key, delta)

  private[this] def idx(key: String) = {
    val entry = circle.ceilingEntry(hash(key))
    val client = if (entry ne null) entry.getValue
    else circle.firstEntry.getValue
    client
  }
}
