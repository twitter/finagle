package com.twitter.finagle.http.codec

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.http.{Fields, Method, Request, Response, Status, Version}
import com.twitter.finagle.http.Status._
import com.twitter.logging.Logger
import com.twitter.util.Future

/**
 * Ensure that the `Response` is a legal response for the request that generated it
 *
 * This attempts to ensure that the HTTP/1.x protocol doesn't get corrupted and
 * result in interesting errors at the peers end which can be difficult to diagnose.
 *
 * TODO: 1xx, 204, and 304 responses are not allowed to have bodies which is
 * not currently enforced https://tools.ietf.org/html/rfc7230#section-3.3.3
 */
private[codec] object ResponseConformanceFilter extends SimpleFilter[Request, Response] {

  private[this] val logger = Logger.get(this.getClass.getName)

  override def apply(request: Request, service: Service[Request, Response]): Future[Response] = {
    service(request).map { rep =>
      // Because our Response is mutable we can perform all the actions we
      // need as side effects. This is not necessarily a good thing.
      validate(request, rep)
      rep
    }
  }

  /**
   * Performs common cleanup tasks to ensure compliance with the HTTP specification
   */
  private[this] def validate(req: Request, rep: Response): Unit = {
    if (req.method == Method.Head) {
      handleHeadResponse(req, rep)
    } else if (mustNotIncludeMessageBody(rep.status)) {
      handleNoMessageResponse(rep)
    } else if (rep.isChunked) {
      handleChunkedResponse(rep)
    } else {
      handleFullyBufferedResponse(rep)
    }
  }

  /**
   * 1. To conform to the RFC, a message body is removed if a status code is
   *    either 1xx, 204 or 304.
   *
   * RFC7230 section-3.3: (https://tools.ietf.org/html/rfc7230#section-3.3)
   * "All 1xx (Informational), 204 (No Content), and 304 (Not Modified) responses
   * do not include a message body."
   *
   * 2. Additionally, a Content-Length header field is dropped for 1xx and 204 responses
   * as described in RFC7230 section-3.3.2. It, however, is allowed to send a Content-Length
   * header field in a 304 response. To follow the section, we don't remove the header field
   * from a 304 response but its value is not checked nor corrected.
   *
   * RFC7230 section-3.3.2: (https://tools.ietf.org/html/rfc7230#section-3.3.2)
   * "A server MUST NOT send a Content-Length header field in any response with a status code
   * of 1xx (Informational) or 204 (No Content)."
   *
   * "A server MAY send a Content-Length header field in a 304 (Not Modified) response to
   * a conditional GET request (Section 4.1 of [RFC7232]); a server MUST NOT send Content-Length
   * in such a response unless its field-value equals the decimal number of octets that would have
   * been sent in the payload body of a 200 (OK) response to the same request."
   */
  private[this] def handleNoMessageResponse(rep: Response): Unit = {
    val contentLength = rep.length
    if (contentLength > 0) {
      rep.clearContent()
      logger.info(
        "Response with a status code of %d must not have a body-message but it has " +
          "a %d-byte payload, thus the content has been removed.",
        rep.statusCode,
        contentLength
      )
    }

    if (rep.status != NotModified) {
      if (rep.contentLength.isDefined) {
        val contentLength = rep.contentLengthOrElse(-1)
        rep.headerMap.remove(Fields.ContentLength)
        logger.info(
          "Response with a status code of %d must not have a Content-Length header field " +
            "thus the field has been removed. Content-Length: %d",
          rep.statusCode,
          contentLength
        )
      }
    }
  }

  private def mustNotIncludeMessageBody(status: Status): Boolean = status match {
    case NoContent | NotModified => true
    case _ if 100 <= status.code && status.code < 200 => true
    case _ => false
  }

  private[this] def handleFullyBufferedResponse(rep: Response): Unit = {
    // Set the Content-Length header to the length of the body
    // if it is not already defined. Examples of reasons that a service might
    // define a Content-Length header to something other than the actual message
    // length include responding to a HEAD request with what the length that the
    // body would have been had it been a GET request.
    if (rep.contentLength.isEmpty) {
      rep.contentLength = rep.content.length
    }
  }

  private[this] def handleChunkedResponse(rep: Response): Unit = {
    // Evaluation order for managing chunked Responses
    // - If the message is HTTP/1.0 any Transfer-Encoding headers are stripped
    //   since HTTP/1.0 doesn't know about transfer-encoding.
    //   https://tools.ietf.org/html/rfc7230#appendix-A.1.3
    // - If we have a Transfer-Encoding header it overrides a Content-Length
    //   header by removing it. https://tools.ietf.org/html/rfc7230#section-3.3.3
    // - If we have a Content-Length header we assume its accurate and don't
    //   use chunked transfer-encoding.
    // - If we don't have a content-length header and are not HTTP/1.0 we add
    //   the 'transfer-encoding: chunked' header.
    if (rep.version == Version.Http10) {
      // HTTP/1.0 doesn't have a notion of Transfer-Encoding
      rep.headerMap.remove(Fields.TransferEncoding)
    } else if (rep.headerMap.getAll(Fields.TransferEncoding).contains("chunked")) {
      rep.headerMap.remove(Fields.ContentLength)
    } else if (!rep.headerMap.contains(Fields.ContentLength)) {
      rep.headerMap.addUnsafe(Fields.TransferEncoding, "chunked")
    }
  }

  /**
   * Ensure consistency of the [[Response]] for HEAD requests
   *
   * RFC-7231: (https://tools.ietf.org/html/rfc7231#section-4.3.2)
   * "The HEAD method is identical to GET except that the server MUST NOT
   * send a message body in the response (i.e., the response terminates at
   * the end of the header section)."
   *
   * Because we don't control encoding ourselves, the `Response` is prepared
   * such that it will be well formed if the downstream encoder behaves as follows:
   * - The outbound transport MUST NOT add content-length or content-encoding
   *   headers as part of the message encoding process.
   * - Non-chunked messages MUST be interpreted as the final element of a HTTP
   *   response.
   */
  private[this] def handleHeadResponse(request: Request, response: Response): Unit = {
    // Netty4 cant encode a HEAD response with a 'transfer-encoding: chunked' header, so we omit
    // payload headers content-encoding from chunked responses as allowed by RFC-7231 section 4.3.2
    response.headerMap.remove(Fields.TransferEncoding)

    if (response.isChunked) {

      // This previously removed the content-length field to conform with
      // https://tools.ietf.org/html/rfc7230#section-3.3.2. However, it was found
      // that since netty doesn't know what type of request each response belongs
      // to, it would mistake some HEAD responses as chunked GET responses, due
      // to the lack of body. This resulted in "chunked" HEAD responses and the
      // content-length header was then incorrectly stripped here.
      // This means that if a user incorrectly sets both chunked transfer encoding
      // and content-length, the content-length will pass through.

      // Make sure we don't leave any writers hanging in case they simply called `close`.
      response.reader.discard()

      // By setting the response to non-chunked, it will be written as a complete
      // response by the HTTP pipeline
      response.setChunked(false)
    }

    if (!response.content.isEmpty) {
      logger.info(
        "Received response to HEAD request (%s) that contained a static body of length %d. " +
          "Discarding body. If this is desired behavior, consider adding HeadFilter to your service",
        request.toString,
        response.content.length
      )

      // Might as well salvage a content length header
      if (response.contentLength.isEmpty) {
        response.contentLength = response.content.length
      }

      // clear the content from the body: otherwise it's a protocol error
      response.clearContent()
    }
  }
}
