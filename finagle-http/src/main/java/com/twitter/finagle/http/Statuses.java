package com.twitter.finagle.http;

/**
 * Java friendly versions of {@link com.twitter.finagle.http.Status}.
 */
public final class Statuses {
  private Statuses() { }

  public static final Status CONTINUE =
    Status$.MODULE$.Continue();
  public static final Status SWITCHING_PROTOCOLS =
    Status$.MODULE$.SwitchingProtocols();
  public static final Status PROCESSING =
    Status$.MODULE$.Processing();
  public static final Status OK =
    Status$.MODULE$.Ok();
  public static final Status CREATED =
    Status$.MODULE$.Created();
  public static final Status ACCEPTED =
    Status$.MODULE$.Accepted();
  public static final Status NON_AUTHORITATIVE_INFORMATION =
    Status$.MODULE$.NonAuthoritativeInformation();
  public static final Status NO_CONTENT =
    Status$.MODULE$.NoContent();
  public static final Status RESET_CONTENT =
    Status$.MODULE$.ResetContent();
  public static final Status PARTIAL_CONTENT =
    Status$.MODULE$.PartialContent();
  public static final Status MULTI_STATUS =
    Status$.MODULE$.MultiStatus();
  public static final Status MULTIPLE_CHOICES =
    Status$.MODULE$.MultipleChoices();
  public static final Status MOVED_PERMANENTLY =
    Status$.MODULE$.MovedPermanently();
  public static final Status FOUND =
    Status$.MODULE$.Found();
  public static final Status SEE_OTHER =
    Status$.MODULE$.SeeOther();
  public static final Status NOT_MODIFIED =
    Status$.MODULE$.NotModified();
  public static final Status USE_PROXY =
    Status$.MODULE$.UseProxy();
  public static final Status TEMPORARY_REDIRECT =
    Status$.MODULE$.TemporaryRedirect();
  public static final Status BAD_REQUEST =
    Status$.MODULE$.BadRequest();
  public static final Status UNAUTHORIZED =
    Status$.MODULE$.Unauthorized();
  public static final Status PAYMENT_REQUIRED =
    Status$.MODULE$.PaymentRequired();
  public static final Status FORBIDDEN =
    Status$.MODULE$.Forbidden();
  public static final Status NOT_FOUND =
    Status$.MODULE$.NotFound();
  public static final Status METHOD_NOT_ALLOWED =
    Status$.MODULE$.MethodNotAllowed();
  public static final Status NOT_ACCEPTABLE =
    Status$.MODULE$.NotAcceptable();
  public static final Status PROXY_AUTHENTICATION_REQUIRED =
    Status$.MODULE$.ProxyAuthenticationRequired();
  public static final Status REQUEST_TIMEOUT =
    Status$.MODULE$.RequestTimeout();
  public static final Status CONFLICT =
    Status$.MODULE$.Conflict();
  public static final Status GONE =
    Status$.MODULE$.Gone();
  public static final Status LENGTH_REQUIRED =
    Status$.MODULE$.LengthRequired();
  public static final Status PRECONDITION_FAILED =
    Status$.MODULE$.PreconditionFailed();
  public static final Status REQUEST_ENTITY_TOO_LARGE =
    Status$.MODULE$.RequestEntityTooLarge();
  public static final Status REQUEST_URITOO_LONG =
    Status$.MODULE$.RequestURITooLong();
  public static final Status UNSUPPORTED_MEDIA_TYPE =
    Status$.MODULE$.UnsupportedMediaType();
  public static final Status REQUESTED_RANGE_NOT_SATISFIABLE =
    Status$.MODULE$.RequestedRangeNotSatisfiable();
  public static final Status EXPECTATION_FAILED =
    Status$.MODULE$.ExpectationFailed();
  public static final Status ENHANCE_YOUR_CALM =
    Status$.MODULE$.EnhanceYourCalm();
  public static final Status UNPROCESSABLE_ENTITY =
    Status$.MODULE$.UnprocessableEntity();
  public static final Status LOCKED =
    Status$.MODULE$.Locked();
  public static final Status FAILED_DEPENDENCY =
    Status$.MODULE$.FailedDependency();
  public static final Status UNORDERED_COLLECTION =
    Status$.MODULE$.UnorderedCollection();
  public static final Status UPGRADE_REQUIRED =
    Status$.MODULE$.UpgradeRequired();
  public static final Status PRECONDITION_REQUIRED =
    Status$.MODULE$.PreconditionRequired();
  public static final Status TOO_MANY_REQUESTS =
    Status$.MODULE$.TooManyRequests();
  public static final Status REQUEST_HEADER_FIELDS_TOO_LARGE =
    Status$.MODULE$.RequestHeaderFieldsTooLarge();
  public static final Status CLIENT_CLOSED_REQUEST =
    Status$.MODULE$.ClientClosedRequest();
  public static final Status INTERNAL_SERVER_ERROR =
    Status$.MODULE$.InternalServerError();
  public static final Status NOT_IMPLEMENTED =
    Status$.MODULE$.NotImplemented();
  public static final Status BAD_GATEWAY =
    Status$.MODULE$.BadGateway();
  public static final Status SERVICE_UNAVAILABLE =
    Status$.MODULE$.ServiceUnavailable();
  public static final Status GATEWAY_TIMEOUT =
    Status$.MODULE$.GatewayTimeout();
  public static final Status HTTP_VERSION_NOT_SUPPORTED =
    Status$.MODULE$.HttpVersionNotSupported();
  public static final Status VARIANT_ALSO_NEGOTIATES =
    Status$.MODULE$.VariantAlsoNegotiates();
  public static final Status INSUFFICIENT_STORAGE =
    Status$.MODULE$.InsufficientStorage();
  public static final Status NOT_EXTENDED =
    Status$.MODULE$.NotExtended();
  public static final Status NETWORK_AUTHENTICATION_REQUIRED =
    Status$.MODULE$.NetworkAuthenticationRequired();

  public static Status fromCode(int code) {
    return Status$.MODULE$.fromCode(code);
  }
}
