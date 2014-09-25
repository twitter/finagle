package com.twitter.finagle.tracing

import com.twitter.finagle._

private[finagle] object TracingFilter {
  val role = Stack.Role("Tracer")

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.tracing.TracingFilter]].
   */
  @deprecated("Use TraceInitializerFilter and (Client|Server)TracingFilter", "6.20.0")
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Simple[ServiceFactory[Req, Rep]] {
      val role = TracingFilter.role
      val description = "Handle span lifecycle events to report tracing from protocols"
      def make(next: ServiceFactory[Req, Rep])(implicit params: Params) = {
        val param.Tracer(tracer) = get[param.Tracer]
        val param.Label(label) = get[param.Label]
        val tracingFilter = new TracingFilter[Req,Rep](tracer, label)
        tracingFilter andThen next
      }
    }
}

/**
 * The TracingFilter takes care of span lifecycle events. It is always
 * placed first in the server filter chain so that protocols with
 * trace support will override the span resets, and still be properly
 * reported here.
 *
 * @param tracer An instance of a tracer to use. Eg: ZipkinTracer
 * @param label The name of the service being traced
 */
@deprecated("Use TraceInitializerFilter and (Client|Server)TracingFilter", "6.20.0")
class TracingFilter[Req, Rep](tracer: Tracer, label: String) extends SimpleFilter[Req, Rep] {

  @deprecated("Please add a label to the tracing filter constructor", "6.13.x")
  def this(tracer: Tracer) = this(tracer, "Unknown")

  def apply(request: Req, service: Service[Req, Rep]) = {
    Trace.unwind {
      Trace.pushTracerAndSetNextId(tracer)
      Trace.recordBinary("finagle.version", Init.finagleVersion)
      Trace.recordServiceName(label)
      service(request)
    }
  }
}

private[finagle] object TraceInitializerFilter {
  val role = Stack.Role("TraceInitializerFilter")

  private[finagle] class Module[Req, Rep](newId: Boolean) extends Stack.Simple[ServiceFactory[Req, Rep]] {
    def this() = this(true)
    val role = TraceInitializerFilter.role
    val description = "Initialize the tracing system"
    def make(next: ServiceFactory[Req, Rep])(implicit params: Params) = {
      val param.Tracer(tracer) = get[param.Tracer]
      val traceInitializer = new TraceInitializerFilter[Req,Rep](tracer, newId)
      traceInitializer andThen next
    }
  }

  /**
   * Create a new stack module for clients. On each request a
   * [[com.twitter.finagle.tracing.Tracer]] will pushed and the next TraceId will be set
   */
  def clientModule[Req, Rep] = new Module[Req, Rep](true)

  /**
   * Create a new stack module for servers. On each request a
   * [[com.twitter.finagle.tracing.Tracer]] will pushed.
   */
  def serverModule[Req, Rep] = new Module[Req, Rep](false)

  def empty[Req, Rep] = new Stack.Simple[ServiceFactory[Req, Rep]] {
    val role = TraceInitializerFilter.role
    val description = "Empty Stackable, used Default(Client|Server)"
    def make(next: ServiceFactory[Req, Rep])(implicit params: Params) = next
  }
}

/**
 * The TraceInitializerFilter takes care of span lifecycle events. It is always
 * placed first in the service [[com.twitter.finagle.Filter]] chain (or last in
 * the [[com.twitter.finagle.Stack]]) so that protocols with trace support will
 * override the span resets, and still be properly reported here.
 *
 * @note This should be replaced by per-codec trace initializers that
 * is capable of parsing trace information out of the codec.
 *
 * @param tracer An instance of a tracer to use. Eg: ZipkinTracer
 * @param newId Set the next TraceId when the tracer is pushed (used for clients)
 */
class TraceInitializerFilter[Req, Rep](tracer: Tracer, newId: Boolean) extends SimpleFilter[Req, Rep] {
  def apply(request: Req, service: Service[Req, Rep]) = {
    Trace.unwind {
      if (newId) Trace.pushTracerAndSetNextId(tracer)
      else Trace.pushTracer(tracer)
      service(request)
    }
  }
}

/**
 * A generic filter that can be used for annotating the Server and Client side
 * of a trace. Finagle-specific trace information should live here.
 *
 * @param label The given name of the service
 * @param before An [[com.twitter.finagle.tracing.Annotation]] to be recorded
 * before the service is called
 * @param after An [[com.twitter.finagle.tracing.Annotation]] to be recorded
 * after the service's [[com.twitter.util.Future]] is satisfied.
 */
sealed class AnnotatingTracingFilter[Req, Rep](
  label: String,
  before: Annotation,
  after: Annotation
) extends SimpleFilter[Req, Rep] {
  def apply(request: Req, service: Service[Req, Rep]) = {
    if (Trace.isActivelyTracing) {
      Trace.recordBinary("finagle.version", Init.finagleVersion)
      Trace.recordServiceName(label)
      Trace.record(before)
      service(request) ensure {
        Trace.record(after)
      }
    } else {
      service(request)
    }
  }
}

/**
 * Annotate the request with Server specific records (ServerRecv, ServerSend)
 */
private[finagle] object ServerTracingFilter {
  val role = Stack.Role("ServerTracingFilter")

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Simple[ServiceFactory[Req, Rep]] {
      val role = ServerTracingFilter.role
      val description = "Report finagle information and server recv/send events"
      def make(next: ServiceFactory[Req, Rep])(implicit params: Params) = {
        val param.Label(label) = get[param.Label]
        val tracingFilter = new AnnotatingTracingFilter[Req, Rep](
          label, Annotation.ServerRecv(), Annotation.ServerSend())
        tracingFilter andThen next
      }
    }
}

/**
 * Annotate the request with Client specific records (ClientSend, ClientRecv)
 */
private[finagle] object ClientTracingFilter {
  val role = Stack.Role("ClientTracingFilter")

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Simple[ServiceFactory[Req, Rep]] {
      val role = ClientTracingFilter.role
      val description = "Report finagle information and client send/recv events"
      def make(next: ServiceFactory[Req, Rep])(implicit params: Params) = {
        val param.Label(label) = get[param.Label]
        val tracingFilter = new AnnotatingTracingFilter[Req, Rep](
          label, Annotation.ClientSend(), Annotation.ClientRecv())
        tracingFilter andThen next
      }
    }
}
