package com.twitter.finagle

/**
 * Finagle-stream implements a rather peculiar protocol: it streams
 * discrete messages delineated by HTTP chunks. It isn't how we'd design
 * a protocol to stream messages, but we are stuck with it for legacy
 * reasons.
 * 
 * Finagle-stream sessions are also ``one-shot``: each session handles
 * exactly one stream. The session terminates together with the stream.
 */
package object stream {
  implicit val streamRequestType = new RequestType[StreamRequest] {
    def canonize(req: StreamRequest) = req
    def specialize(req: StreamRequest) = req
  }
}
