package com.twitter.finagle.service

import com.twitter.util.Try

/**
 * Represents a request/response pair.
 *
 * For some protocols, like HTTP, these types are what you'd expect —
 * `com.twitter.finagle.http.Request` and `com.twitter.finagle.http.Response`.
 * While for other protocols that may not be the case. Please review
 * the protocol's
 * "com.twitter.finagle.$protocol.service.$ProtocolResponseClassifier"
 * for details.
 *
 * @see `com.twitter.finagle.http.service.HttpResponseClassifier`
 * @see `com.twitter.finagle.thriftmux.service.ThriftMuxResponseClassifier`
 * @see [[ReqRepT]] for a type-safe version
 */
object ReqRep {
  def apply(req: Any, rep: Try[Any]): ReqRep = ReqRepT[Any, Any](req, rep)
  def unapply(reqRep: ReqRep): Option[(Any, Try[Any])] = Some((reqRep.request, reqRep.response))
}

/**
 * A type-safe request/response pair.
 *
 * @see [[ReqRep]] for untyped
 */
case class ReqRepT[Req, Rep](request: Req, response: Try[Rep]) extends ReqRep {
  type Request = Req
  type Response = Rep
}

// keeps backward compatibility behavior for when ReqRep was a case class,
// we can't leak generics here without risking major breaking changes.
// this can go away when we move everything to ReqRepT version.
sealed trait ReqRep {
  type Request
  type Response
  def request: Request
  def response: Try[Response]
}
