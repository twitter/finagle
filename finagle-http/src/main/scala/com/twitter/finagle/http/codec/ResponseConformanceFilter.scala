package com.twitter.finagle.http.codec

import com.twitter.finagle.{Service, SimpleFilter}
import com.twitter.finagle.http._
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

  override def apply(
    request: Request,
    service: Service[Request, Response]
  ): Future[Response] = {
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
    } else if (rep.isChunked) {
      handleChunkedResponse(rep)
    } else {
      handleFullyBufferedResponse(rep)
    }
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
    rep.headerMap.set(Fields.TransferEncoding, "chunked")

    // We remove any content-length headers because "A sender MUST NOT
    // send a Content-Length header field in any message that contains
    // a Transfer-Encoding header field."
    // https://tools.ietf.org/html/rfc7230#section-3.3.2
    rep.headerMap.remove(Fields.ContentLength)
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
      // This was intended to be a chunked response, so we "MUST NOT" include a content-length
      // header: https://tools.ietf.org/html/rfc7230#section-3.3.2
      response.headerMap.remove(Fields.ContentLength)

      // Make sure we don't leave any writers hanging in case they simply called `close`.
      response.reader.discard()

      // By setting the response to non-chunked, it will be written as a complete
      // response by the HTTP pipeline
      response.setChunked(false)
    }

    if (!response.content.isEmpty) {
      logger.error(
          "Received response to HEAD request (%s) that contained a static body of length %d. " +
            "Discarding body. If this is desired behavior, consider adding HeadFilter to your service",
          request.toString, response.content.length)

      // Might as well salvage a content length header
      if (response.contentLength.isEmpty) {
        response.contentLength = response.content.length
      }

      // clear the content from the body: otherwise it's a protocol error
      response.clearContent()
    }
  }
}
