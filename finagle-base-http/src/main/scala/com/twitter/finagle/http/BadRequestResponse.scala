package com.twitter.finagle.http

/**
 * Utility for generating a response to various forms of bad requests, with
 * the only distinction being different status codes. The generated responses
 * contain all the information it needs to be a valid HTTP 1.0 response such as
 * the Content-Length and Connection headers.
 */
private[finagle] object BadRequestResponse {

  def apply(): Response = baseResponse(Status.BadRequest)

  def contentTooLong(): Response = baseResponse(Status.RequestEntityTooLarge)

  def uriTooLong(): Response = baseResponse(Status.RequestURITooLong)

  def headerTooLong(): Response = baseResponse(Status.RequestHeaderFieldsTooLarge)

  // Prepare a barebones response with the given status
  // We likely won't be performing any HTTP conformance validation so we
  // need to make sure the response has all the information it needs such as
  // the Content-Length and Connection headers
  private[this] def baseResponse(status: Status): Response = {
    val resp = Response(status = status, version = Version.Http10)
    resp.contentLength = 0
    resp.headerMap.setUnsafe(Fields.Connection, "close")
    resp
  }
}
