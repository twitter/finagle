package com.twitter.finagle.tracing

import com.twitter.finagle._
import com.twitter.util.Future
import com.twitter.util.ResourceTracker
import com.twitter.util.Throw

object TraceInitializerFilter {
  val role: Stack.Role = Stack.Role("TraceInitializerFilter")

  /**
   * @param newId Set the next TraceId when the tracer is pushed, `true` for clients.
   */
  private[finagle] def apply[Req, Rep](tracer: Tracer, newId: Boolean): Filter[Req, Rep, Req, Rep] =
    new TraceInitializerFilter[Req, Rep](tracer, newId)

  private[finagle] def typeAgnostic(tracer: Tracer, newId: Boolean): Filter.TypeAgnostic =
    new Filter.TypeAgnostic {
      def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = apply(tracer, newId)
    }

  private[finagle] class Module[Req, Rep](newId: Boolean)
      extends Stack.Module1[param.Tracer, ServiceFactory[Req, Rep]] {
    def this() = this(true)
    val role: Stack.Role = TraceInitializerFilter.role
    val description = "Initialize the tracing system"
    def make(_tracer: param.Tracer, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
      apply(_tracer.tracer, newId).andThen(next)
    }
  }

  /**
   * Create a new stack module for clients. On each request a
   * [[com.twitter.finagle.tracing.Tracer]] will pushed and the next TraceId will be set
   */
  private[finagle] def clientModule[Req, Rep] = new Module[Req, Rep](true)

  /**
   * Create a new stack module for servers. On each request a
   * [[com.twitter.finagle.tracing.Tracer]] will pushed.
   */
  private[finagle] def serverModule[Req, Rep] = new Module[Req, Rep](false)

  private[finagle] def empty[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module0[ServiceFactory[Req, Rep]] {
      val role: Stack.Role = TraceInitializerFilter.role
      val description = "Empty Stackable, used Default(Client|Server)"
      def make(next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = next
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
class TraceInitializerFilter[Req, Rep](tracer: Tracer, newId: Boolean)
    extends SimpleFilter[Req, Rep] {
  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] =
    if (tracer.isNull) {
      if (newId) Trace.letId(Trace.nextId) { service(request) }
      else service(request)
    } else {
      if (newId) Trace.letTracerAndNextId(tracer) { service(request) }
      else
        Trace.letTracer(tracer) { service(request) }
    }
}

/**
 * A generic filter that can be used for annotating the Server and Client side
 * of a trace. Finagle-specific trace information should live here.
 *
 * @param label The given name of the service
 * @param prefix A prefix for `finagle.version`, `dtab.local`, & `dtab.limited`.
 * [[com.twitter.finagle.tracing.Annotation Annotation]] keys.
 * @param before An [[com.twitter.finagle.tracing.Annotation]] to be recorded
 * before the service is called
 * @param after An [[com.twitter.finagle.tracing.Annotation]] to be recorded
 * after the service's [[com.twitter.util.Future]] is satisfied, regardless of success.
 * @param afterFailure Function from String to [[com.twitter.finagle.tracing.Annotation]] to be recorded
 * if the service's [[com.twitter.util.Future]] fails.
 * @param finagleVersion A thunk that returns the version of finagle. Useful
 * for testing.
 */
sealed class AnnotatingTracingFilter[Req, Rep](
  label: String,
  prefix: String,
  before: Annotation,
  after: Annotation,
  afterFailure: String => Annotation,
  finagleVersion: () => String = () => Init.finagleVersion,
  traceMetadata: Boolean = true)
    extends SimpleFilter[Req, Rep] {
  def this(
    label: String,
    before: Annotation,
    after: Annotation,
    afterFailure: String => Annotation
  ) =
    this(label, "unknown", before, after, afterFailure)

  def this(label: String, before: Annotation, after: Annotation) = {
    this(label, "unknown", before, after, AnnotatingTracingFilter.defaultAfterFailureTracer)
  }

  private[this] val finagleVersionKey = s"$prefix/finagle.version"
  private[this] val dtabLocalKey = s"$prefix/dtab.local"
  private[this] val dtabLimitedKey = s"$prefix/dtab.limited"
  private[this] val labelKey = s"$prefix/finagle.label"

  def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
    val trace = Trace()
    if (trace.isActivelyTracing) {
      if (traceMetadata) {
        trace.recordServiceName(TraceServiceName() match {
          case Some(l) => l
          case None => label
        })
        trace.recordBinary(labelKey, label)
        trace.recordBinary(finagleVersionKey, finagleVersion())
        // Trace dtab propagation on all requests that have them.
        if (Dtab.local.nonEmpty) {
          trace.recordBinary(dtabLocalKey, Dtab.local.show)
        }
        if (Dtab.limited.nonEmpty) {
          trace.recordBinary(dtabLimitedKey, Dtab.limited.show)
        }
      }

      trace.record(before)

      service(request).respond {
        case Throw(e) =>
          trace.record(afterFailure(s"${e.getClass.getName}: ${e.getMessage}"))
          trace.record(after)
        case _ =>
          trace.record(after)
      }
    } else {
      service(request)
    }
  }
}

object AnnotatingTracingFilter {
  private[finagle] final val defaultAfterFailureTracer = { errorStr: String =>
    Annotation.Message(s"Error seen in AnnotatingTracingFilter: $errorStr")
  }
}

/**
 * Annotate the request with Server specific records (ServerRecv, ServerSend)
 */
object ServerTracingFilter {
  val role = Stack.Role("ServerTracingFilter")

  case class TracingFilter[Req, Rep](
    label: String,
    finagleVersion: () => String = () => Init.finagleVersion)
      extends AnnotatingTracingFilter[Req, Rep](
        label,
        "srv",
        Annotation.ServerRecv,
        Annotation.ServerSend,
        Annotation.ServerSendError(_),
        finagleVersion,
        traceMetadata = false
      )

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module2[param.Label, param.Tracer, ServiceFactory[Req, Rep]] {
      val role: Stack.Role = ServerTracingFilter.role
      val description = "Report finagle information and server recv/send events"
      def make(
        _label: param.Label,
        _tracer: param.Tracer,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        val param.Tracer(tracer) = _tracer
        if (tracer.isNull) next
        else {
          val param.Label(label) = _label
          TracingFilter[Req, Rep](label).andThen(next)
        }
      }
    }
}

/**
 * Annotate the request with Client specific records (ClientSend, ClientRecv)
 */
object ClientTracingFilter {
  val role = Stack.Role("ClientTracingFilter")

  case class TracingFilter[Req, Rep](
    label: String,
    finagleVersion: () => String = () => Init.finagleVersion)
      extends AnnotatingTracingFilter[Req, Rep](
        label,
        "clnt",
        Annotation.ClientSend,
        Annotation.ClientRecv,
        Annotation.ClientRecvError(_),
        finagleVersion
      )

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module2[param.Label, param.Tracer, ServiceFactory[Req, Rep]] {
      val role: Stack.Role = ClientTracingFilter.role
      val description = "Report finagle information and client send/recv events"
      def make(
        _label: param.Label,
        _tracer: param.Tracer,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        val param.Tracer(tracer) = _tracer
        if (tracer.isNull) next
        else {
          val param.Label(label) = _label
          TracingFilter[Req, Rep](label).andThen(next)
        }
      }
    }
}

/**
 * Annotate the low level resource utilization of traced requests such as the
 * accumulated cpu time of executed continuations.
 */
object ResourceTracingFilter {
  private val Role: Stack.Role = Stack.Role("ResourceTracingFilter")
  private val Description: String = "Trace resource usage of requests"
  private val CpuTimeAnnotationKey: String = "srv/finagle_cputime_ns"
  private val ContinuationsAnnotationKey: String = "srv/finagle_continuations_executed"

  private object ResourceUsageFilter extends Filter.TypeAgnostic {
    def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = new SimpleFilter[Req, Rep] {
      def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
        val trace = Trace()
        if (!trace.isActivelyTracing) service(request)
        else if (!ResourceTracker.threadCpuTimeSupported) {
          trace.recordBinary(CpuTimeAnnotationKey, "unsupported")
          service(request)
        } else {
          ResourceTracker { accumulator =>
            // We first calculate the synchronous work on `req`.
            val serviceApplyStart = ResourceTracker.currentThreadCpuTime
            val response = service(request)
            val serviceApplyTime = ResourceTracker.currentThreadCpuTime - serviceApplyStart

            response.ensure {
              val cpuTime = serviceApplyTime + accumulator.totalCpuTime
              trace.recordBinary(CpuTimeAnnotationKey, cpuTime)
              trace.recordBinary(ContinuationsAnnotationKey, accumulator.numContinuations)
            }
          }
        }
      }
    }
  }

  def serverModule[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] = {
    new Stack.Module1[param.Tracer, ServiceFactory[Req, Rep]] {
      val role = Role
      val description = Description
      def make(_tracer: param.Tracer, next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] = {
        val param.Tracer(tracer) = _tracer
        if (tracer.isNull) next
        else ResourceUsageFilter.andThen(next)
      }
    }
  }
}

/**
 * Annotate the request events directly before/after sending data on the wire (WireSend, WireRecv)
 */
object WireTracingFilter {
  private[finagle] val role = Stack.Role("WireTracingFilter")

  case class TracingFilter[Req, Rep](
    label: String,
    prefix: String,
    before: Annotation,
    after: Annotation,
    traceMetadata: Boolean,
    finagleVersion: () => String = () => Init.finagleVersion)
      extends AnnotatingTracingFilter[Req, Rep](
        label,
        prefix,
        before,
        after,
        Annotation.WireRecvError(_),
        finagleVersion,
        traceMetadata
      )

  private def module[Req, Rep](
    prefix: String,
    before: Annotation,
    after: Annotation,
    traceMetadata: Boolean
  ): Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module2[param.Label, param.Tracer, ServiceFactory[Req, Rep]] {
      val role: Stack.Role = WireTracingFilter.role
      val description = "Report finagle information and wire send/recv events"
      def make(
        _label: param.Label,
        _tracer: param.Tracer,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        val param.Tracer(tracer) = _tracer
        if (tracer.isNull) next
        else {
          val param.Label(label) = _label
          TracingFilter[Req, Rep](label, prefix, before, after, traceMetadata).andThen(next)
        }
      }
    }

  private[finagle] def clientModule[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] = module(
    "clnt",
    Annotation.WireSend,
    Annotation.WireRecv,
    traceMetadata = false
  )

  private[finagle] def serverModule[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] = module(
    "srv",
    Annotation.WireRecv,
    Annotation.WireSend,
    traceMetadata = true
  )
}

/**
 * A filter to clear tracing information
 */
class TracelessFilter extends Filter.TypeAgnostic {
  override def toFilter[Req, Rep]: Filter[Req, Rep, Req, Rep] = new SimpleFilter[Req, Rep] {
    def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
      Trace.letClear {
        service(request)
      }
    }
  }
}
