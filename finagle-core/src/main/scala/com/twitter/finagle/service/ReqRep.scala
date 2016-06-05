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
 */
case class ReqRep(request: Any, response: Try[Any])
