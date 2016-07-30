package com.twitter.finagle.redis.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.Future

private[redis] class RedisLoggingFilter(stats: StatsReceiver) extends SimpleFilter[Command, Reply] {

  private[this] val error = stats.scope("error")
  private[this] val succ  = stats.scope("success")

  override def apply(command: Command, service: Service[Command, Reply]): Future[Reply] = {
    service(command).onSuccess {
      case StatusReply(_)
           | IntegerReply(_)
           | BulkReply(_)
           | EmptyBulkReply
           | MBulkReply(_)
           | NilMBulkReply
           | EmptyMBulkReply   => succ.counter(command.command).incr()
      case ErrorReply(message) => error.counter(command.command).incr()
      case _                   => error.counter(command.command).incr()
    }
  }
}
