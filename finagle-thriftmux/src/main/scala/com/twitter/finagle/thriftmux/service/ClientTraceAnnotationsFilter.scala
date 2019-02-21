package com.twitter.finagle.thriftmux.service

import com.twitter.finagle.Stack.Role
import com.twitter.finagle.{Service, ServiceFactory, SimpleFilter, Stack, Stackable, mux}
import com.twitter.finagle.thrift.ClientDeserializeCtx
import com.twitter.finagle.thriftmux.service.ClientTraceAnnotationFilter.RequestSerializationMissing
import com.twitter.finagle.tracing.Trace
import com.twitter.util.Future

private[finagle] object ClientTraceAnnotationsFilter {

  /**
   * A Stack module which pulls data out of [[ClientDeserializeCtx]] to record
   * as annotations in the [[Trace]].
   */
  def module: Stackable[ServiceFactory[mux.Request, mux.Response]] = {
    new Stack.Module0[ServiceFactory[mux.Request, mux.Response]] {
      private[this] val recordFilter = new SimpleFilter[mux.Request, mux.Response] {
        def apply(
          request: mux.Request,
          service: Service[mux.Request, mux.Response]
        ): Future[mux.Response] =
          service(request).ensure {
            val deserCtx = ClientDeserializeCtx.get
            if (deserCtx ne ClientDeserializeCtx.nullDeserializeCtx) {
              val trace = Trace()
              if (trace.isActivelyTracing) {
                deserCtx.rpcName match {
                  case Some(name) => trace.recordRpc(name)
                  case _ =>
                }
                val serNs = deserCtx.serializationTime
                if (serNs >= 0L)
                  trace.recordBinary("clnt/request_serialization_ns", serNs)

                val deserNs = deserCtx.deserializationTime
                if (deserNs >= 0L) {
                  trace.recordBinary("clnt/response_deserialization_ns", deserNs)
                  // There are specific cases where serialization is negative and deserialization
                  // is positive that we cannot explain. "Log" these instances so we can debug them.
                  if (serNs == RequestSerializationMissing)
                    trace.recordBinary("clnt/request_serialization_missing", 1)
                  else if (serNs < 0)
                    trace.recordBinary("clnt/request_serialization_measurement_error", serNs)
                }
              }
            }
          }
      }

      val role: Role = Role("trace annotations")
      val description: String = "records client's tracing information"
      def make(
        next: ServiceFactory[mux.Request, mux.Response]
      ): ServiceFactory[mux.Request, mux.Response] =
        recordFilter.andThen(next)
    }
  }
}

private[finagle] object ClientTraceAnnotationFilter {
  val RequestSerializationMissing: Long = Long.MinValue
}
