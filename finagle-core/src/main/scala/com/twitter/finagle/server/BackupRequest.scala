package com.twitter.finagle.server

import com.twitter.finagle.{Service, ServiceFactory, SimpleFilter, Stack, Stackable, param}
import com.twitter.finagle.tracing.{Annotation, Trace}
import com.twitter.util.Future

private[finagle] object BackupRequest {

  private val Role: Stack.Role = Stack.Role("BackupRequestProcessingAnnotation")
  private val annotation =
    Annotation.BinaryAnnotation("srv/backup_request_processing", true)

  /**
   * Stack module used server-side for adding trace annotations which note
   * when requests were initiated as a backup request.
   */
  private[finagle] def traceAnnotationModule[Req, Rep]: Stackable[ServiceFactory[Req, Rep]] =
    new Stack.Module1[param.Tracer, ServiceFactory[Req, Rep]] {
      def role: Stack.Role = Role
      def description: String = "Adds a tracing annotation when processing a backup request"

      private[this] val annotater = new SimpleFilter[Req, Rep] {
        override def toString: String = "BackupRequestProcessingAnnotationFilter"
        def apply(request: Req, service: Service[Req, Rep]): Future[Rep] = {
          if (com.twitter.finagle.context.BackupRequest.wasInitiated) {
            val tracing = Trace()
            if (tracing.isActivelyTracing) {
              tracing.record(annotation)
            }
          }
          service(request)
        }
      }

      def make(
        tracerParam: param.Tracer,
        next: ServiceFactory[Req, Rep]
      ): ServiceFactory[Req, Rep] = {
        if (tracerParam.tracer.isNull) {
          next
        } else {
          annotater.andThen(next)
        }
      }
    }

}
