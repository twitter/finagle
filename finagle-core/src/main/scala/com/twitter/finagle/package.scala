package com.twitter

import com.twitter.finagle.stats.DefaultStatsReceiver
import com.twitter.finagle.toggle.{StandardToggleMap, ToggleMap}
import scala.util.control.NoStackTrace

/**

Finagle is an extensible RPC system.

Services are represented by class [[com.twitter.finagle.Service]]. Clients make use of
[[com.twitter.finagle.Service]] objects while servers implement them.

Finagle contains a number of protocol implementations; each of these implement
[[com.twitter.finagle.Client Client]] and/or [[com.twitter.finagle.Server]]. For example, Finagle's
HTTP implementation, [[com.twitter.finagle.Http]] (in package `finagle-http`), exposes both.

Thus a simple HTTP server is built like this:

{{{
import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.util.{Await, Future}

val service = new Service[Request, Response] {
  def apply(req: Request): Future[Response] =
    Future.value(Response())
}
val server = Http.server.serve(":8080", service)
Await.ready(server)
}}}

We first define a service to which requests are dispatched. In this case, the service returns
immediately with a HTTP 200 OK response, and with no content.

This service is then served via the Http protocol on TCP port 8080. Finally we wait for the server
to stop serving.

We can now query our web server:

{{{
% curl -D - localhost:8080
HTTP/1.1 200 OK
}}}

Building an HTTP client is also simple. (Note that type annotations are added for illustration.)

{{{
import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.http.{Request, Response}
import com.twitter.util.{Future, Return, Throw}

val client: Service[Request, Response] = Http.client.newService("localhost:8080")
val f: Future[Response] = client(Request()).respond {
  case Return(rep) =>
    printf("Got HTTP response %s\n", rep)
  case Throw(exc) =>
    printf("Got error %s\n", exc)
}
}}}

`Http.client.newService("localhost:8080")` constructs a new [[com.twitter.finagle.Service]] instance
connected to localhost TCP port 8080. We then issue a HTTP/1.1 GET request to URI "/". The service
returns a [[com.twitter.util.Future]] representing the result of the operation. We listen to this
future, printing an appropriate message when the response arrives.

The [[https://twitter.github.io/finagle/ Finagle homepage]] contains useful documentation and
resources for using Finagle.
 */
package object finagle {
  object stack {
    object Endpoint extends Stack.Role("Endpoint")

    /**
     * Creates a [[com.twitter.finagle.Stack]] which always fails.
     */
    def nilStack[Req, Rep]: Stack[ServiceFactory[Req, Rep]] =
      Stack.leaf(
        Stack.Role("nil"),
        new com.twitter.finagle.service.FailingFactory[Req, Rep](
          UnterminatedStackException
        )
      )

    private[this] object UnterminatedStackException
        extends IllegalArgumentException("Unterminated stack")
        with NoStackTrace
  }

  private[this] val LibraryName: String = "com.twitter.finagle.core"

  /**
   * The [[ToggleMap]] used for finagle-core
   */
  private[finagle] val CoreToggles: ToggleMap =
    StandardToggleMap(LibraryName, DefaultStatsReceiver)
}
