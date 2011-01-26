package com.twitter.finagle.kestrel

import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.util._
import com.twitter.conversions.time._
import com.twitter.finagle.Service
import com.twitter.finagle.kestrel.protocol._
import com.twitter.finagle.memcached.util.ChannelBufferUtils._

object Client {
  def apply(raw: Service[Command, Response]): Client = {
    new ConnectedClient(raw)
  }
}

trait Client {
  def set(queueName: String, value: ChannelBuffer, expiry: Time = Time.epoch): Future[Response]
  def get(queueName: String, waitUpTo: Duration = 0.seconds): Future[Option[ChannelBuffer]]
  def delete(queueName: String): Future[Response]
  def flush(queueName: String): Future[Response]
  def receive(queueName: String, waitUpTo: Duration = 0.seconds)(f: ChannelBuffer => Unit): Task
}

class Task {
  private[this] var _isCancelled = false

  def cancel() {
    _isCancelled = true
  }

  def isCancelled = _isCancelled
}

protected class ConnectedClient(underlying: Service[Command, Response]) extends Client {
  def flush(queueName: String) = {
    underlying(Flush(queueName))
  }

  def delete(queueName: String) = {
    underlying(Delete(queueName))
  }

  def set(queueName: String, value: ChannelBuffer, expiry: Time = Time.epoch) = {
    underlying(Set(queueName, expiry, value))
  }

  def get(queueName: String, waitUpTo: Duration = 0.seconds) = {
    underlying(Get(queueName, collection.Set(Timeout(waitUpTo)))) map {
      case Values(Seq()) => None
      case Values(Seq(Value(key, value))) => Some(value)
    }
  }

  def receive(queueName: String, waitUpTo: Duration = 0.seconds)(f: ChannelBuffer => Unit): Task = {
    val task = new Task
    receive0(queueName: String, task, waitUpTo, collection.Set(Open()), f)
    task
  }

  private[this] def receive0(queueName: String, task: Task, waitUpTo: Duration, options: collection.Set[GetOption], f: ChannelBuffer => Unit) {
    if (!task.isCancelled) {
      underlying(Get(queueName, collection.Set(Timeout(waitUpTo)) ++ options)) respond {
        case Return(Values(Seq(Value(key, item)))) =>
          val options = collection.mutable.Set[GetOption]()
          options += Open()
          try {
            f(item)
            options += Close()
          } catch {
            case e => options += Abort()
          }
          receive0(queueName, task, waitUpTo, options, f)
        case Return(Values(Seq())) =>
          receive0(queueName, task, waitUpTo, collection.Set(Open()), f)
        case Throw(e) =>
          // unsure -- FIXME!
      }
    }
  }
}