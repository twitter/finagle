package com.twitter.finagle.memcached

import com.twitter.finagle.{Service, ServiceFactory, SimpleFilter, Stack}
import com.twitter.finagle.memcached.protocol.{Command, Response, RetrievalCommand, Values}
import com.twitter.finagle.param
import com.twitter.finagle.tracing.Trace
import com.twitter.io.Buf
import com.twitter.util.{Future, Return}
import scala.collection.mutable

private[finagle] object MemcachedTracingFilter {

  object Module extends Stack.Module1[param.Label, ServiceFactory[Command, Response]] {
    val role: Stack.Role = Stack.Role("MemcachedTracing")
    val description: String = "Add Memcached client specific annotations to the trace"

    def make(
      _label: param.Label,
      next: ServiceFactory[Command, Response]
    ): ServiceFactory[Command, Response] = {
      TracingFilter.andThen(next)
    }
  }

  object TracingFilter extends SimpleFilter[Command, Response] {
    def apply(command: Command, service: Service[Command, Response]): Future[Response] = {
      val trace = Trace()
      val response = service(command)
      if (trace.isActivelyTracing) {
        // Submitting rpc name here assumes there is no further tracing lower in the stack
        trace.recordRpc(command.name)
        command match {
          case command: RetrievalCommand =>
            response.respond {
              case Return(Values(vals)) =>
                val misses = mutable.Set.empty[String]
                command.keys.foreach {
                  case Buf.Utf8(key) =>
                    misses += key
                }
                vals.foreach { value =>
                  val Buf.Utf8(key) = value.key
                  trace.recordBinary(key, "Hit")
                  misses.remove(key)
                }
                misses.foreach {
                  trace.recordBinary(_, "Miss")
                }
              case _ =>
            }
          case _ =>
            response
        }
      } else {
        response
      }
    }
  }
}
