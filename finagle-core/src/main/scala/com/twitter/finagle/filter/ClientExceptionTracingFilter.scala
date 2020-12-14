package com.twitter.finagle.filter

import com.twitter.finagle._
import com.twitter.finagle.tracing.{Trace, Tracing}
import com.twitter.util.{Future, Return, Throw}

/**
 * Reports error and exception annotations when a span completes.
 */
class ClientExceptionTracingFilter[Req, Rep] extends SimpleFilter[Req, Rep] {
  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val tracing = Trace()
    val rep = service(request)
    if (!tracing.isActivelyTracing) rep
    else handleResponse(tracing, rep)
  }

  protected def handleResponse(tracing: Tracing, rep: Future[Rep]): Future[Rep] = {
    rep.respond {
      case Throw(error) =>
        traceError(tracing, error)
      case Return(_) =>
    }
  }

  final protected def traceError(tracing: Tracing, error: Throwable): Unit = {
    tracing.recordBinary("error", true)
    tracing.recordBinary("exception.type", error.getClass.getTypeName)
    tracing.recordBinary("exception.message", error.getMessage)
  }
}

private[finagle] object ClientExceptionTracingFilter {
  val role = Stack.Role("ExceptionTracing")

  /**
   * Creates a [[com.twitter.finagle.Stackable]]
   * [[com.twitter.finagle.filter.ClientExceptionTracingFilter]].
   */
  def module[Req, Rep](
    filter: SimpleFilter[Req, Rep] = new ClientExceptionTracingFilter[Req, Rep]
  ): Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module0[ServiceFactory[Req, Rep]] {
      val role = ClientExceptionTracingFilter.role
      val description = "Record error annotations for completed spans"
      def make(next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        filter andThen next
      }
    }
}
