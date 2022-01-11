package com.twitter.finagle.thrift

import com.twitter.finagle.tracing.Trace

// Internal helper for adding annotations to server calls.
// Both the scala and java backends want to add tracing annotations for thrift
// methods and this provides a central place to consolidate that logic to
// make it both more sharable and reduce the amount of generated code.
object ServerAnnotations {

  private[this] def getCurrentClientId: String = ClientId.current match {
    case Some(id) => id.name
    case None => "N/A"
  }

  def annotate(rpcName: String, endpointName: String): Unit = {
    val trace = Trace()
    if (trace.isActivelyTracing) {
      trace.recordRpc(rpcName)
      trace.recordBinary("srv/thrift_endpoint", endpointName)
      trace.recordBinary("srv/clientId", getCurrentClientId)
    }
  }
}
