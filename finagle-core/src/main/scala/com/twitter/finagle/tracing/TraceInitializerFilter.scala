package com.twitter.finagle.tracing

import com.twitter.finagle._

private[finagle] object TracingFilter {
  val role = Stack.Role("Tracer")

  /**
   * Creates a [[com.twitter.finagle.Stackable]] [[com.twitter.finagle.tracing.TracingFilter]].
   */
  @deprecated("Use TraceInitializerFilter and (Client|Server)TracingFilter", "6.20.0")
  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module2[param.Tracer, param.Label, ServiceFactory[Req, Rep]] {
      val role = TracingFilter.role
      val description = "Handle span lifecycle events to report tracing from protocols"
      def make(_tracer: param.Tracer, _label: param.Label, next: ServiceFactory[Req, Rep]) = {
        val param.Tracer(tracer) = _tracer
        val param.Label(label) = _label
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

  private[finagle] class Module[Req, Rep](newId: Boolean)
    extends Stack.Module1[param.Tracer, ServiceFactory[Req, Rep]] {
    def this() = this(true)
    val role = TraceInitializerFilter.role
    val description = "Initialize the tracing system"
    def make(_tracer: param.Tracer, next: ServiceFactory[Req, Rep]) = {
      val param.Tracer(tracer) = _tracer
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

  def empty[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module0[ServiceFactory[Req, Rep]] {
      val role = TraceInitializerFilter.role
      val description = "Empty Stackable, used Default(Client|Server)"
      def make(next: ServiceFactory[Req, Rep]) = next
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
 * @param prefix A prefix for `finagle.version` and `dtab.local`.
 * [[com.twitter.finagle.tracing.Annotation Annotation]] keys.
 * @param before An [[com.twitter.finagle.tracing.Annotation]] to be recorded
 * before the service is called
 * @param after An [[com.twitter.finagle.tracing.Annotation]] to be recorded
 * after the service's [[com.twitter.util.Future]] is satisfied.
 * @param finagleVersion A thunk that returns the version of finagle. Useful
 * for testing.
 */
sealed class AnnotatingTracingFilter[Req, Rep](
  label: String,
  prefix: String,
  before: Annotation,
  after: Annotation,
  finagleVersion: () => String = () => Init.finagleVersion
) extends SimpleFilter[Req, Rep] {
  def this(label: String, before: Annotation, after: Annotation) =
    this(label, "unknown", before, after)

  private[this] val finagleVersionKey = s"$prefix/finagle.version"
  private[this] val dtabLocalKey = s"$prefix/dtab.local"

  def apply(request: Req, service: Service[Req, Rep]) = {
    if (Trace.isActivelyTracing) {
      Trace.recordServiceName(label)
      Trace.recordBinary(finagleVersionKey, finagleVersion())
      // Trace dtab propagation on all requests that have them.
      if (Dtab.local.nonEmpty) {
        Trace.recordBinary(dtabLocalKey, Dtab.local.show)
      }
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

  case class TracingFilter[Req, Rep](
    label: String,
    finagleVersion: () => String = () => Init.finagleVersion
  ) extends AnnotatingTracingFilter[Req, Rep](label, "srv",
    Annotation.ServerRecv(), Annotation.ServerSend(), finagleVersion)

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Label, ServiceFactory[Req, Rep]] {
      val role = ServerTracingFilter.role
      val description = "Report finagle information and server recv/send events"
      def make(_label: param.Label, next: ServiceFactory[Req, Rep]) = {
        val param.Label(label) = _label
        TracingFilter[Req, Rep](label) andThen next
      }
    }
}

/**
 * Annotate the request with Client specific records (ClientSend, ClientRecv)
 */
private[finagle] object ClientTracingFilter {
  val role = Stack.Role("ClientTracingFilter")

  case class TracingFilter[Req, Rep](
    label: String,
    finagleVersion: () => String = () => Init.finagleVersion
  ) extends AnnotatingTracingFilter[Req, Rep](label, "clnt",
      Annotation.ClientSend(), Annotation.ClientRecv(), finagleVersion)

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Label, ServiceFactory[Req, Rep]] {
      val role = ClientTracingFilter.role
      val description = "Report finagle information and client send/recv events"
      def make(_label: param.Label, next: ServiceFactory[Req, Rep]) = {
        val param.Label(label) = _label
        TracingFilter[Req, Rep](label) andThen next
      }
    }
}
