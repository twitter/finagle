package com.twitter.finagle.http.filter

import com.twitter.finagle._
import com.twitter.finagle.tracing.{Trace, Tracing}
import com.twitter.util.Future

private[finagle] object HttpTracingFilter extends SimpleFilter[http.Request, http.Response] {
  val Role: Stack.Role = Stack.Role("HttpTracing")
  def apply(
    request: http.Request,
    service: Service[http.Request, http.Response]
  ): Future[http.Response] = {
    val tracing = Trace()
    val rep = service(request)
    traceHttpRequest(request, tracing)
    traceHttpResponse(rep, tracing)
    rep
  }

  def traceHttpRequest(request: http.Request, tracing: Tracing): Unit = {
    if (tracing.isActivelyTracing) {
      tracing.recordRpc(request.method.toString)
      tracing.recordBinary("http.method", request.method.toString)
      tracing.recordBinary("http.uri", stripParameters(request.uri))
    }
  }

  def traceHttpResponse(rep: Future[http.Response], tracing: Tracing): Unit = {
    if (tracing.isActivelyTracing) {
      rep.onSuccess { r =>
        tracing.recordBinary("http.status_code", r.statusCode)
      }
    }
  }

  /**
   * Remove any parameters from url.
   */
  private[this] def stripParameters(uri: String): String = {
    uri.indexOf('?') match {
      case -1 => uri
      case n => uri.substring(0, n)
    }
  }

  /**
   * Creates a [[com.twitter.finagle.Stackable]] `HttpTracingFilter` which will add binary
   * annotations `http.uri`, `http.status_code`, and `http.method` to completed http spans.
   */
  def module: Stackable[ServiceFactory[http.Request, http.Response]] =
    new Stack.Module0[ServiceFactory[http.Request, http.Response]] {
      val role = HttpTracingFilter.Role
      val description = "Record http annotation for completed spans"
      def make(
        next: ServiceFactory[http.Request, http.Response]
      ): ServiceFactory[http.Request, http.Response] = {
        HttpTracingFilter.andThen(next)
      }
    }
}
