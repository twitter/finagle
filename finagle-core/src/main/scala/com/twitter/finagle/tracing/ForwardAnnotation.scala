package com.twitter.finagle.tracing

import com.twitter.finagle.context.Contexts
import com.twitter.finagle.tracing.Annotation.BinaryAnnotation
import com.twitter.finagle.Service
import com.twitter.finagle.ServiceFactory
import com.twitter.finagle.SimpleFilter
import com.twitter.finagle.Stack
import com.twitter.finagle.Stackable
import com.twitter.util.Future

/**
 * Allow us to inject annotations into children trace contexts. Example, with dark
 * traffic or retries we want to be able to tag the produced spans from each call
 * (the light/dark calls, or each individual retry) with information that allows us
 * to tie them together as a single logical item: a light request paired with a dark
 * request, or an initial request and a series of retries. The problem lies in the fact
 * that at the time we have the information about the relationship, the
 * child trace context doesn't actually yet exist. In order to trace where it is needed, we need
 * to have a hook into the trace context of these children requests which don't yet exist.
 * We do this by placing annotations we want each child trace context to carry inside the local
 * context and accessing them when the child trace context is realized through a stack module. The
 * stack module will place a filter in the correct trace context, after the Trace Initialization
 * Filter, which allows us to record the annotations in the proper place.
 */
object ForwardAnnotation {

  private val ChildAnnotationsKey: Contexts.local.Key[Seq[BinaryAnnotation]] =
    new Contexts.local.Key[Seq[BinaryAnnotation]]

  class ForwardAnnotationFilter[Req, Rep]() extends SimpleFilter[Req, Rep] {
    def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
      val trace = Trace()
      Contexts.local.get(ChildAnnotationsKey).foreach { annotations =>
        annotations.foreach(trace.record)
      }

      Contexts.local.letClear(ChildAnnotationsKey) {
        service(request)
      }
    }
  }

  /**
   * Apply a [[BinaryAnnotation]] to a subsequent child trace. An example might
   * be to apply an identifier to two requests so that they can be identified
   * as being related in the resulting trace.
   */
  private[finagle] def let[R](annotation: BinaryAnnotation)(fn: => R): R = {
    val annotations = Contexts.local.get(ChildAnnotationsKey).getOrElse(Seq())
    Contexts.local.let(ChildAnnotationsKey, annotations :+ annotation)(fn)
  }

  /**
   * Apply a series of [[BinaryAnnotation]]s to a subsequent child trace.
   */
  private[finagle] def let[R](annotation: Seq[BinaryAnnotation])(fn: => R): R = {
    val annotations = Contexts.local.get(ChildAnnotationsKey).getOrElse(Seq())
    Contexts.local.let(ChildAnnotationsKey, annotations ++ annotation)(fn)
  }

  /**
   * Return a list of [[BinaryAnnotation]]s applied by [[ForwardAnnotationFilter]].
   * If no [[BinaryAnnotation]]s are found, return None.
   */
  private[twitter] def current: Option[Seq[BinaryAnnotation]] =
    Contexts.local.get(ChildAnnotationsKey)

  def module[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] = {
    new Stack.Module0[ServiceFactory[Req, Rep]] {
      val role: Stack.Role = Stack.Role("ChildTraceContext")
      val description: String = "Apply BinaryAnnotations from parent context to current span"
      def make(next: ServiceFactory[Req, Rep]): ServiceFactory[Req, Rep] =
        new ForwardAnnotationFilter[Req, Rep].andThen(next)
    }
  }

}
