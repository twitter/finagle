package com.twitter.finagle.redis.filter

import com.twitter.finagle._
import com.twitter.finagle.redis.protocol._
import com.twitter.finagle.redis.util.BufToString
import com.twitter.finagle.stats.StatsReceiver
import com.twitter.util.{Future, Return}

private[finagle] object RedisLoggingFilter {
  val role: Stack.Role = Stack.Role("RedisStatsFilter")
  val description: String = "Redis Stats"

  def module: Stackable[ServiceFactory[Command, Reply]] =
    new Stack.Module1[param.Stats, ServiceFactory[Command, Reply]] {
      val role: Stack.Role = RedisLoggingFilter.role
      val description: String = RedisLoggingFilter.description

      def make(
        statsParam: param.Stats,
        next: ServiceFactory[Command, Reply]
      ): ServiceFactory[Command, Reply] = {
        if (statsParam.statsReceiver.isNull)
          next
        else
          new RedisLoggingFilter(statsParam.statsReceiver).andThen(next)
      }
    }
}

private[finagle] class RedisLoggingFilter(stats: StatsReceiver)
    extends SimpleFilter[Command, Reply] {

  private[this] val error = stats.scope("error")
  private[this] val succ = stats.scope("success")

  override def apply(command: Command, service: Service[Command, Reply]): Future[Reply] = {
    service(command).respond {
      case Return(
            StatusReply(_) | IntegerReply(_) | BulkReply(_) | EmptyBulkReply | MBulkReply(_) |
            NilMBulkReply | EmptyMBulkReply
          ) =>
        succ.counter(BufToString(command.name)).incr()
      case Return(ErrorReply(message)) =>
        error.counter(BufToString(command.name)).incr()
      case Return(_) =>
        error.counter(BufToString(command.name)).incr()
      case _ =>
    }
  }
}
