package com.twitter.finagle.http

import org.jboss.netty.handler.codec.http.HttpResponseStatus

/** Scala aliases for HttpResponseStatus.  Java users should use Netty's HttpResponseStatus. */
object Status {
  val Continue                     = HttpResponseStatus.CONTINUE
  val Processing                   = HttpResponseStatus.PROCESSING
  val Ok                           = HttpResponseStatus.OK
  val Created                      = HttpResponseStatus.CREATED
  val Accepted                     = HttpResponseStatus.ACCEPTED
  val NonAuthoritativeInformation  = HttpResponseStatus.NON_AUTHORITATIVE_INFORMATION
  val NoContent                    = HttpResponseStatus.NO_CONTENT
  val ResetContent                 = HttpResponseStatus.RESET_CONTENT
  val PartialContent               = HttpResponseStatus.PARTIAL_CONTENT
  val MultiStatus                  = HttpResponseStatus.MULTI_STATUS
  val MultipleChoices              = HttpResponseStatus.MULTIPLE_CHOICES
  val MovedPermanently             = HttpResponseStatus.MOVED_PERMANENTLY
  val Found                        = HttpResponseStatus.FOUND
  val SeeOther                     = HttpResponseStatus.SEE_OTHER
  val NotModified                  = HttpResponseStatus.NOT_MODIFIED
  val UseProxy                     = HttpResponseStatus.USE_PROXY
  val TemporaryRedirect            = HttpResponseStatus.TEMPORARY_REDIRECT
  val BadRequest                   = HttpResponseStatus.BAD_REQUEST
  val Unauthorized                 = HttpResponseStatus.UNAUTHORIZED
  val PaymentRequired              = HttpResponseStatus.PAYMENT_REQUIRED
  val Forbidden                    = HttpResponseStatus.FORBIDDEN
  val NotFound                     = HttpResponseStatus.NOT_FOUND
  val MethodNotAllowed             = HttpResponseStatus.METHOD_NOT_ALLOWED
  val NotAcceptable                = HttpResponseStatus.NOT_ACCEPTABLE
  val ProxyAuthenticationRequired  = HttpResponseStatus.PROXY_AUTHENTICATION_REQUIRED
  val RequestTimeout               = HttpResponseStatus.REQUEST_TIMEOUT
  val Conflict                     = HttpResponseStatus.CONFLICT
  val Gone                         = HttpResponseStatus.GONE
  val LengthRequired               = HttpResponseStatus.LENGTH_REQUIRED
  val PreconditionFailed           = HttpResponseStatus.PRECONDITION_FAILED
  val RequestEntityTooLarge        = HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE
  val RequestUriTooLong            = HttpResponseStatus.REQUEST_URI_TOO_LONG
  val UnsupportedMediaType         = HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE
  val RequestedRangeNotSatisfiable = HttpResponseStatus.REQUESTED_RANGE_NOT_SATISFIABLE
  val ExpectationFailed            = HttpResponseStatus.EXPECTATION_FAILED
  val UnprocessableEntity          = HttpResponseStatus.UNPROCESSABLE_ENTITY
  val Locked                       = HttpResponseStatus.LOCKED
  val FailedDependency             = HttpResponseStatus.FAILED_DEPENDENCY
  val UnorderedCollection          = HttpResponseStatus.UNORDERED_COLLECTION
  val UpgradeRequired              = HttpResponseStatus.UPGRADE_REQUIRED
  val InternalServerError          = HttpResponseStatus.INTERNAL_SERVER_ERROR
  val NotImplemented               = HttpResponseStatus.NOT_IMPLEMENTED
  val BadGateway                   = HttpResponseStatus.BAD_GATEWAY
  val ServiceUnavailable           = HttpResponseStatus.SERVICE_UNAVAILABLE
  val GatewayTimeout               = HttpResponseStatus.GATEWAY_TIMEOUT
  val HttpVersionNotSupported      = HttpResponseStatus.HTTP_VERSION_NOT_SUPPORTED
  val VariantAlsoNegotiates        = HttpResponseStatus.VARIANT_ALSO_NEGOTIATES
  val InsufficientStorage          = HttpResponseStatus.INSUFFICIENT_STORAGE
  val NotExtended                  = HttpResponseStatus.NOT_EXTENDED
}
