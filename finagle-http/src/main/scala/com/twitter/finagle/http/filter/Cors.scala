package com.twitter.finagle.http.filter

import com.twitter.finagle.{Filter, Service}
import com.twitter.finagle.http.{Request, Response, Status, Method}
import com.twitter.util.{Duration, Future}

/** Implements http://www.w3.org/TR/cors/ */
object Cors {
  /**
   * A Cross-Origin Resource Sharing policy.
   *
   * A Policy determines how CORS response headers are set in response to a request with
   * CORS headers:
   *
   * allowsOrigin is a function that takes the value specified in the Origin request header
   * and optionally returns the value of Access-Control-Allow-Origin.
   *
   * allowsMethods is a function that takes the value of the Access-Control-Request-Method
   * preflight request header and optionally returns a list of methods to be set in the
   * Access-Control-Allow-Methods response header.
   *
   * allowsHeaders is a function that takes the values set in the Access-Control-Request-Headers
   * preflight request header and returns the header names to be set in the Access-Control-Allow-
   * Headers response header.
   *
   * exposedHeaders is the list of header names to be set in the Access-Control-Expose-Headers
   * response header (in response to non-preflight requests).
   *
   * If supportsCredentials is true and allowsOrigin does not return '*', the Access-Control-
   * Allow-Credentials resopnse header will be set to 'true'.
   *
   * If maxAge is defined, its value (in seconds) will be set in the Access-Control-Max-Age
   * response header.
   */
  case class Policy(
    allowsOrigin: String => Option[String],
    allowsMethods: String => Option[Seq[String]],
    allowsHeaders: Seq[String] => Option[Seq[String]],
    exposedHeaders: Seq[String] = Nil,
    supportsCredentials: Boolean = false,
    maxAge: Option[Duration] = None)

  /** A CORS policy that lets you do whatever you want.  Don't use this in production. */
  val UnsafePermissivePolicy: Policy = Policy(
    allowsOrigin  = { origin  => Some(origin) },
    allowsMethods = { method  => Some(method :: Nil) },
    allowsHeaders = { headers => Some(headers) },
    supportsCredentials = true)

  /**
   * An HTTP filter that handles preflight (OPTIONS) requests and sets CORS response headers
   * as described in the W3C CORS spec.
   */
  class HttpFilter(policy: Policy)
      extends Filter[Request, Response, Request, Response] {

    /*
     * Simple Cross-Origin Request, Actual Request, and Redirects
     */

    protected[this] def getOrigin(request: Request): Option[String] = {
      /*
       * If the Origin header is not present terminate this set of steps. The request is outside
       * the scope of this specification.
       */
      Option(request.headers.get("Origin")) flatMap { origin =>
        /*
         * If the value of the Origin header is not a case-sensitive match for any of the values
         * in list of origins, do not set any additional headers and terminate this set of steps.
         */
        policy.allowsOrigin(origin)
      }
    }

    /**
     * If the resource supports credentials add a single Access-Control-Allow-Origin
     * header, with the value of the Origin header as value, and add a single
     * Access-Control-Allow-Credentials header with the case-sensitive string "true" as
     * value.
     *
     * Otherwise, add a single Access-Control-Allow-Origin header, with either the value
     * of the Origin header or the string "*" as value.
     *
     * n.b. The string "*" cannot be used for a resource that supports credentials.
     */
    protected[this] def setOriginAndCredentials(response: Response, origin: String): Response = {
      response.headers.add("Access-Control-Allow-Origin", origin)
      if (policy.supportsCredentials && origin != "*") {
        response.headers.add("Access-Control-Allow-Credentials", "true")
      }
      response
    }

    /**
     * Resources that wish to enable themselves to be shared with multiple Origins but do not
     * respond uniformly with "*" must in practice generate the Access-Control-Allow-Origin header
     * dynamically in response to every request they wish to allow. As a consequence, authors of
     * such resources should send a Vary: Origin HTTP header or provide other appropriate control
     * directives to prevent caching of such responses, which may be inaccurate if re-used across-
     * origins.
     */
    def setVary(response: Response): Response = {
      response.headers.set("Vary", "Origin")
      response
    }

    /**
     * If the list of exposed headers is not empty add one or more Access-Control-Expose-
     * Headers headers, with as values the header field names given in the list of exposed
     * headers.
     *
     * By not adding the appropriate headers resource can also clear the preflight result
     * cache of all entries where origin is a case-sensitive match for the value of the
     * Origin header and url is a case-sensitive match for the URL of the resource.
     */
    protected[this] def addExposedHeaders(response: Response): Response = {
      if (policy.exposedHeaders.nonEmpty) {
        response.headers.add(
          "Access-Control-Expose-Headers", policy.exposedHeaders.mkString(", "))
      }
      response
    }

    /** http://www.w3.org/TR/cors/#resource-requests */
    protected[this] def handleSimple(request: Request, response: Response): Response =
      getOrigin(request) map {
        setOriginAndCredentials(response, _)
      } map {
        addExposedHeaders(_)
      } getOrElse response

    /*
     * Preflight (OPTIONS) requests
     */

    protected[this] object Preflight {
      def unapply(request: Request): Boolean =
        request.method == Method.Options
    }

    /** Let method be the value as result of parsing the Access-Control-Request-Method header. */
    protected[this] def getMethod(request: Request): Option[String] =
      Option(request.headers.get("Access-Control-Request-Method"))

    /**
     * If method is a simple method this step may be skipped.
     *
     * Add one or more Access-Control-Allow-Methods headers consisting of (a subset of) the list of
     * methods.
     */
    protected[this] def setMethod(response: Response, methods: Seq[String]): Response = {
      response.headers.set("Access-Control-Allow-Methods", methods.mkString(", "))
      response
    }

    /**
     * Optionally add a single Access-Control-Max-Age header with as value the amount of seconds
     * the user agent is allowed to cache the result of the request.
     */
    protected[this] def setMaxAge(response: Response): Response = {
      policy.maxAge foreach { maxAge =>
        response.headers.add("Access-Control-Max-Age", maxAge.inSeconds.toString)
      }
      response
    }

    private[this] val commaSpace = ", *".r

    /**
     * Let header field-names be the values as result of parsing the
     * Access-Control-Request-Headers headers. If there are no Access-Control-Request-Headers
     * headers let header field-names be the empty list.
     */
    protected[this] def getHeaders(request: Request): Seq[String] =
      Option(request.headers.get("Access-Control-Request-Headers")) map {
        commaSpace.split(_).toSeq
      } getOrElse List.empty[String]

    /**
     * If each of the header field-names is a simple header and none is Content-Type, than this step
     * may be skipped.
     *
     * Add one or more Access-Control-Allow-Headers headers consisting of (a subset of) the list of
     * headers.
     */
    protected[this] def setHeaders(response: Response, headers: Seq[String]): Response = {
      if (headers.nonEmpty) {
        response.headers.set("Access-Control-Allow-Headers", headers.mkString(", "))
      }
      response
    }

    /** http://www.w3.org/TR/cors/#resource-preflight-requests */
    protected[this] def handlePreflight(request: Request): Option[Response] =
      getOrigin(request) flatMap { origin =>
        getMethod(request) flatMap { method =>
          val headers = getHeaders(request)
          policy.allowsMethods(method) flatMap { allowedMethods =>
            policy.allowsHeaders(headers) map { allowedHeaders =>
              setHeaders(
                setMethod(
                  setMaxAge(
                    setOriginAndCredentials(request.response, origin)),
                  allowedMethods),
                allowedHeaders)
            }
          }
        }
      }

    /**
     * Fully handle preflight requests.  If a preflight request is deemed to be unacceptable,
     * a 200 OK response is served without CORS headers.
     *
     * Adds CORS response headers onto all non-preflight requests that have the 'Origin' header
     * set to a value that is allowed by the Policy.
     */
    def apply(request: Request, service: Service[Request, Response]): Future[Response] = {
      val response = request match {
        case Preflight() => Future {
          // If preflight is not acceptable, just return a 200 without CORS headers
          handlePreflight(request) getOrElse request.response
        }
        case _ => service(request) map { handleSimple(request, _) }
      }
      response map { setVary(_) }
    }
  }
}

/**
 * Adds headers to support Cross-origin resource sharing.
 *
 * This is here for backwards compatibility.  You should probably use Cors.HttpFilter directly.
 */
object CorsFilter {
  private[this] val sep = ", *".r

  def apply(origin:  String = "*",
            methods: String = "GET",
            headers: String = "x-requested-with",
            exposes: String = ""): Filter[Request, Response, Request, Response] = {
    val methodList = Some(sep.split(methods).toSeq)
    val headerList = Some(sep.split(headers).toSeq)
    val exposeList = sep.split(exposes).toSeq
    new Cors.HttpFilter(Cors.Policy(
      { _ => Some(origin) },
      { _ => methodList },
      { _ => headerList },
      exposeList))
  }
}
