package com.twitter.finagle.http;

/**
 * Java friendly versions of {@link com.twitter.finagle.http.Status}.
 *
 * @deprecated as of 2017-02-13. Please use the {@link com.twitter.finagle.http.Status}
 * instances directly.
 */
@Deprecated
public final class Statuses {
  private Statuses() { }

  public static final Status CONTINUE =
    Status.Continue();
  public static final Status SWITCHING_PROTOCOLS =
    Status.SwitchingProtocols();
  public static final Status PROCESSING =
    Status.Processing();
  public static final Status OK =
    Status.Ok();
  public static final Status CREATED =
    Status.Created();
  public static final Status ACCEPTED =
    Status.Accepted();
  public static final Status NON_AUTHORITATIVE_INFORMATION =
    Status.NonAuthoritativeInformation();
  public static final Status NO_CONTENT =
    Status.NoContent();
  public static final Status RESET_CONTENT =
    Status.ResetContent();
  public static final Status PARTIAL_CONTENT =
    Status.PartialContent();
  public static final Status MULTI_STATUS =
    Status.MultiStatus();
  public static final Status MULTIPLE_CHOICES =
    Status.MultipleChoices();
  public static final Status MOVED_PERMANENTLY =
    Status.MovedPermanently();
  public static final Status FOUND =
    Status.Found();
  public static final Status SEE_OTHER =
    Status.SeeOther();
  public static final Status NOT_MODIFIED =
    Status.NotModified();
  public static final Status USE_PROXY =
    Status.UseProxy();
  public static final Status TEMPORARY_REDIRECT =
    Status.TemporaryRedirect();
  public static final Status BAD_REQUEST =
    Status.BadRequest();
  public static final Status UNAUTHORIZED =
    Status.Unauthorized();
  public static final Status PAYMENT_REQUIRED =
    Status.PaymentRequired();
  public static final Status FORBIDDEN =
    Status.Forbidden();
  public static final Status NOT_FOUND =
    Status.NotFound();
  public static final Status METHOD_NOT_ALLOWED =
    Status.MethodNotAllowed();
  public static final Status NOT_ACCEPTABLE =
    Status.NotAcceptable();
  public static final Status PROXY_AUTHENTICATION_REQUIRED =
    Status.ProxyAuthenticationRequired();
  public static final Status REQUEST_TIMEOUT =
    Status.RequestTimeout();
  public static final Status CONFLICT =
    Status.Conflict();
  public static final Status GONE =
    Status.Gone();
  public static final Status LENGTH_REQUIRED =
    Status.LengthRequired();
  public static final Status PRECONDITION_FAILED =
    Status.PreconditionFailed();
  public static final Status REQUEST_ENTITY_TOO_LARGE =
    Status.RequestEntityTooLarge();
  public static final Status REQUEST_URITOO_LONG =
    Status.RequestURITooLong();
  public static final Status UNSUPPORTED_MEDIA_TYPE =
    Status.UnsupportedMediaType();
  public static final Status REQUESTED_RANGE_NOT_SATISFIABLE =
    Status.RequestedRangeNotSatisfiable();
  public static final Status EXPECTATION_FAILED =
    Status.ExpectationFailed();
  public static final Status ENHANCE_YOUR_CALM =
    Status.EnhanceYourCalm();
  public static final Status UNPROCESSABLE_ENTITY =
    Status.UnprocessableEntity();
  public static final Status LOCKED =
    Status.Locked();
  public static final Status FAILED_DEPENDENCY =
    Status.FailedDependency();
  public static final Status UNORDERED_COLLECTION =
    Status.UnorderedCollection();
  public static final Status UPGRADE_REQUIRED =
    Status.UpgradeRequired();
  public static final Status PRECONDITION_REQUIRED =
    Status.PreconditionRequired();
  public static final Status TOO_MANY_REQUESTS =
    Status.TooManyRequests();
  public static final Status REQUEST_HEADER_FIELDS_TOO_LARGE =
    Status.RequestHeaderFieldsTooLarge();
  public static final Status UNAVAILABLE_FOR_LEGAL_REASONS =
    Status.UnavailableForLegalReasons();
  public static final Status CLIENT_CLOSED_REQUEST =
    Status.ClientClosedRequest();
  public static final Status INTERNAL_SERVER_ERROR =
    Status.InternalServerError();
  public static final Status NOT_IMPLEMENTED =
    Status.NotImplemented();
  public static final Status BAD_GATEWAY =
    Status.BadGateway();
  public static final Status SERVICE_UNAVAILABLE =
    Status.ServiceUnavailable();
  public static final Status GATEWAY_TIMEOUT =
    Status.GatewayTimeout();
  public static final Status HTTP_VERSION_NOT_SUPPORTED =
    Status.HttpVersionNotSupported();
  public static final Status VARIANT_ALSO_NEGOTIATES =
    Status.VariantAlsoNegotiates();
  public static final Status INSUFFICIENT_STORAGE =
    Status.InsufficientStorage();
  public static final Status NOT_EXTENDED =
    Status.NotExtended();
  public static final Status NETWORK_AUTHENTICATION_REQUIRED =
    Status.NetworkAuthenticationRequired();

  public static Status fromCode(int code) {
    return Status.fromCode(code);
  }
}
