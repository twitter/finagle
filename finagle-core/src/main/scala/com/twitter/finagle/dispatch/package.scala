package com.twitter.finagle

import com.twitter.finagle.tracing.TraceId

package object dispatch {

 // type ReqToTraceId = AnyRef => TraceId
//  implicit val implicitReqToTraceId = Stack.Param(ReqToTraceId)
}
