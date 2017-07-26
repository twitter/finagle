package com.twitter.finagle.redis.filter

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.redis.protocol.{Command, Reply}
import com.twitter.finagle.redis.util.BufToString
import com.twitter.finagle.tracing.{Annotation, Trace}
import com.twitter.util.Future

private[redis] class RedisTracingFilter extends SimpleFilter[Command, Reply] {

  private val traceRecv: () => Unit = () => Trace.record(Annotation.ClientRecv())

  override def apply(command: Command, service: Service[Command, Reply]): Future[Reply] = {
    if (Trace.isActivelyTracing) {
      Trace.recordServiceName("redis")
      Trace.recordRpc(BufToString(command.name))
      Trace.record(Annotation.ClientSend())
      service(command).ensure(traceRecv())
    } else service(command)
  }
}
