package com.twitter.finagle.http

import scala.annotation.switch

/**
 * Represents an HTTP status code.
 *
 * The set of commonly known HTTP status codes have an associated reason phrase
 * (see `reasons`). We don't provide a way to set the reason phrase because:
 *
 * - it simplifies construction (users only supply the code)
 * - it avoids the need to validate user-defined reason phrases
 * - it omits the possibility of statuses with duplicate reason phrases
 *
 * The only downside is that we lose the ability to create custom statuses with
 * "vanity" reason phrases, but this should be tolerable.
 */
case class Status(code: Int) {
  def reason: String =
    Status.reasons.get(this) match {
      case Some(reason) => reason
      case _ if code < 100 => "Unknown Status"
      case _ if code < 200 => "Informational"
      case _ if code < 300 => "Successful"
      case _ if code < 400 => "Redirection"
      case _ if code < 500 => "Client Error"
      case _ if code < 600 => "Server Error"
      case _ => "Unknown Status"
    }
}

object Status {

  /**
   * Matches when the status code isn't in range of any of the  categories: {{Informational}},
   * {{Successful}}, {{Redirection}}, {{ClientError}}, {{ServerError}}.
   */
  object Unknown {
    def unapply(status: Status): Option[Status] =
      Some(status).filter(s => s.code < 100 || s.code >= 600)
  }

  /** Matches when the status code is between 100 and 200. */
  object Informational {
    def unapply(status: Status): Option[Status] =
      inRange(100, 200, status)
  }

  /** Matches when the status code is between 200 and 300. */
  object Successful {
    def unapply(status: Status): Option[Status] =
      inRange(200, 300, status)
  }

  /** Matches when the status code is between 300 and 400. */
  object Redirection {
    def unapply(status: Status): Option[Status] =
      inRange(300, 400, status)
  }

  /** Matches when the status code is between 400 and 500. */
  object ClientError {
    def unapply(status: Status): Option[Status] =
      inRange(400, 500, status)
  }

  /** Matches when the status code is between 500 and 600. */
  object ServerError {
    def unapply(status: Status): Option[Status] =
      inRange(500, 600, status)
  }

  private[finagle] def inRange(lower: Int, upper: Int, status: Status): Option[Status] = {
    if (lower <= status.code && status.code < upper) Some(status)
    else None
  }

  val Continue = Status(100)
  val SwitchingProtocols = Status(101)
  val Processing = Status(102)
  val Ok = Status(200)
  val Created = Status(201)
  val Accepted = Status(202)
  val NonAuthoritativeInformation = Status(203)
  val NoContent = Status(204)
  val ResetContent = Status(205)
  val PartialContent = Status(206)
  val MultiStatus = Status(207)
  val MultipleChoices = Status(300)
  val MovedPermanently = Status(301)
  val Found = Status(302)
  val SeeOther = Status(303)
  val NotModified = Status(304)
  val UseProxy = Status(305)
  val TemporaryRedirect = Status(307)
  val PermanentRedirect = Status(308)
  val BadRequest = Status(400)
  val Unauthorized = Status(401)
  val PaymentRequired = Status(402)
  val Forbidden = Status(403)
  val NotFound = Status(404)
  val MethodNotAllowed = Status(405)
  val NotAcceptable = Status(406)
  val ProxyAuthenticationRequired = Status(407)
  val RequestTimeout = Status(408)
  val Conflict = Status(409)
  val Gone = Status(410)
  val LengthRequired = Status(411)
  val PreconditionFailed = Status(412)
  val RequestEntityTooLarge = Status(413)
  val RequestURITooLong = Status(414)
  val UnsupportedMediaType = Status(415)
  val RequestedRangeNotSatisfiable = Status(416)
  val ExpectationFailed = Status(417)
  val EnhanceYourCalm = Status(420)
  val UnprocessableEntity = Status(422)
  val Locked = Status(423)
  val FailedDependency = Status(424)
  val UnorderedCollection = Status(425)
  val UpgradeRequired = Status(426)
  val PreconditionRequired = Status(428)
  val TooManyRequests = Status(429)
  val RequestHeaderFieldsTooLarge = Status(431)
  val UnavailableForLegalReasons = Status(451)
  val ClientClosedRequest = Status(499)
  val InternalServerError = Status(500)
  val NotImplemented = Status(501)
  val BadGateway = Status(502)
  val ServiceUnavailable = Status(503)
  val GatewayTimeout = Status(504)
  val HttpVersionNotSupported = Status(505)
  val VariantAlsoNegotiates = Status(506)
  val InsufficientStorage = Status(507)
  val NotExtended = Status(510)
  val NetworkAuthenticationRequired = Status(511)

  def fromCode(code: Int): Status = {
    (code: @switch) match {
      case 100 => Continue
      case 101 => SwitchingProtocols
      case 102 => Processing
      case 200 => Ok
      case 201 => Created
      case 202 => Accepted
      case 203 => NonAuthoritativeInformation
      case 204 => NoContent
      case 205 => ResetContent
      case 206 => PartialContent
      case 207 => MultiStatus
      case 300 => MultipleChoices
      case 301 => MovedPermanently
      case 302 => Found
      case 303 => SeeOther
      case 304 => NotModified
      case 305 => UseProxy
      case 307 => TemporaryRedirect
      case 308 => PermanentRedirect
      case 400 => BadRequest
      case 401 => Unauthorized
      case 402 => PaymentRequired
      case 403 => Forbidden
      case 404 => NotFound
      case 405 => MethodNotAllowed
      case 406 => NotAcceptable
      case 407 => ProxyAuthenticationRequired
      case 408 => RequestTimeout
      case 409 => Conflict
      case 410 => Gone
      case 411 => LengthRequired
      case 412 => PreconditionFailed
      case 413 => RequestEntityTooLarge
      case 414 => RequestURITooLong
      case 415 => UnsupportedMediaType
      case 416 => RequestedRangeNotSatisfiable
      case 417 => ExpectationFailed
      case 420 => EnhanceYourCalm
      case 422 => UnprocessableEntity
      case 423 => Locked
      case 424 => FailedDependency
      case 425 => UnorderedCollection
      case 426 => UpgradeRequired
      case 428 => PreconditionRequired
      case 429 => TooManyRequests
      case 431 => RequestHeaderFieldsTooLarge
      case 451 => UnavailableForLegalReasons
      case 499 => ClientClosedRequest
      case 500 => InternalServerError
      case 501 => NotImplemented
      case 502 => BadGateway
      case 503 => ServiceUnavailable
      case 504 => GatewayTimeout
      case 505 => HttpVersionNotSupported
      case 506 => VariantAlsoNegotiates
      case 507 => InsufficientStorage
      case 510 => NotExtended
      case 511 => NetworkAuthenticationRequired
      case _ => Status(code)
    }
  }

  // See note in Status.
  private val reasons: Map[Status, String] = Map(
    Continue -> "Continue",
    SwitchingProtocols -> "Switching Protocols",
    Processing -> "Processing",
    Ok -> "OK",
    Created -> "Created",
    Accepted -> "Accepted",
    NonAuthoritativeInformation -> "Non-Authoritative Information",
    NoContent -> "No Content",
    ResetContent -> "Reset Content",
    PartialContent -> "Partial Content",
    MultiStatus -> "Multi-Status",
    MultipleChoices -> "Multiple Choices",
    MovedPermanently -> "Moved Permanently",
    Found -> "Found",
    SeeOther -> "See Other",
    NotModified -> "Not Modified",
    UseProxy -> "Use Proxy",
    TemporaryRedirect -> "Temporary Redirect",
    PermanentRedirect -> "Permanent Redirect",
    BadRequest -> "Bad Request",
    Unauthorized -> "Unauthorized",
    PaymentRequired -> "Payment Required",
    Forbidden -> "Forbidden",
    NotFound -> "Not Found",
    MethodNotAllowed -> "Method Not Allowed",
    NotAcceptable -> "Not Acceptable",
    ProxyAuthenticationRequired -> "Proxy Authentication Required",
    RequestTimeout -> "Request Timeout",
    Conflict -> "Conflict",
    Gone -> "Gone",
    LengthRequired -> "Length Required",
    PreconditionFailed -> "Precondition Failed",
    RequestEntityTooLarge -> "Request Entity Too Large",
    RequestURITooLong -> "Request-URI Too Long",
    UnsupportedMediaType -> "Unsupported Media Type",
    RequestedRangeNotSatisfiable -> "Requested Range Not Satisfiable",
    ExpectationFailed -> "Expectation Failed",
    EnhanceYourCalm -> "Enhance Your Calm",
    UnprocessableEntity -> "Unprocessable Entity",
    Locked -> "Locked",
    FailedDependency -> "Failed Dependency",
    UnorderedCollection -> "Unordered Collection",
    UpgradeRequired -> "Upgrade Required",
    PreconditionRequired -> "Precondition Required",
    TooManyRequests -> "Too Many Requests",
    RequestHeaderFieldsTooLarge -> "Request Header Fields Too Large",
    UnavailableForLegalReasons -> "Unavailable For Legal Reasons",
    ClientClosedRequest -> "Client Closed Request",
    InternalServerError -> "Internal Server Error",
    NotImplemented -> "Not Implemented",
    BadGateway -> "Bad Gateway",
    ServiceUnavailable -> "Service Unavailable",
    GatewayTimeout -> "Gateway Timeout",
    HttpVersionNotSupported -> "HTTP Version Not Supported",
    VariantAlsoNegotiates -> "Variant Also Negotiates",
    InsufficientStorage -> "Insufficient Storage",
    NotExtended -> "Not Extended",
    NetworkAuthenticationRequired -> "Network Authentication Required"
  )
}
