package com.twitter

/**

Finagle is an extensible RPC system.

Services are represented by class [[com.twitter.finagle.Service]].
Clients make use of [[com.twitter.finagle.Service]] objects while
servers implement them.

Finagle contains a number of protocol implementations; each of these
implement [[com.twitter.finagle.Client Client]] and/or
[[com.twitter.finagle.Server]]. For example, finagle's HTTP implementation,
[[com.twitter.finagle.Http]] (in package `finagle-http`), exposes both.

Thus a simple HTTP server is built like this:

{{{
import com.twitter.finagle.{Http, Service}
import org.jboss.netty.handler.codec.http.{
  HttpRequest, HttpResponse, DefaultHttpResponse}
import org.jboss.netty.handler.codec.http.HttpVersion._
import org.jboss.netty.handler.codec.http.HttpResponseStatus._
import com.twitter.util.{Future, Await}

val service = new Service[HttpRequest, HttpResponse] {
  def apply(req: HttpRequest) =
    Future.value(new DefaultHttpResponse(HTTP_1_1, OK))
}
val server = Http.serve(":8080", service)
Await.ready(server)
}}}

We first define a service to which requests are dispatched. In this case,
the service returns immediately with a HTTP 200 OK response, and with
no content.

This service is then served via the Http protocol on TCP port 8080. Finally
we wait for the server to stop serving.

We can now query our web server:

<pre>
% curl -D - localhost:8080
HTTP/1.1 200 OK

%
</pre>

Building an HTTP client is also simple. (Note that type annotations are
added for illustration.)

{{{
import com.twitter.finagle.{Http, Service}
import org.jboss.netty.handler.codec.http.{
  HttpRequest, HttpResponse, DefaultHttpRequest}
import org.jboss.netty.handler.codec.http.HttpVersion._
import org.jboss.netty.handler.codec.http.HttpMethod._
import com.twitter.util.{Future, Return, Throw}

val client: Service[HttpRequest, HttpResponse] =
  Http.newService("localhost:8080")
val f: Future[HttpResponse] =
  client(new DefaultHttpRequest(HTTP_1_1, GET, "/"))
f respond {
  case Return(res) =>
    printf("Got HTTP response %s\n", res)
  case Throw(exc) =>
    printf("Got error %s\n", exc)
}
}}}

`Http.newService("localhost:8080")` constructs a new
[[com.twitter.finagle.Service]] instance connected to
localhost TCP port 8080. We then issue a HTTP/1.1 GET
request to URI "/". The service returns a [[com.twitter.util.Future]]
representing the result of the operation. We listen to
this future, printing an appropriate message when the
response arrives.

The [[http://twitter.github.io/finagle/ Finagle homepage]] contains
useful documentation and resources for using Finagle.

 */
package object finagle {
  object stack {
    object Endpoint extends Stack.Role("Endpoint")
    /**
     * Creates a [[com.twitter.finagle.Stack.Leaf]] which always fails.
     */
    def nilStack[Req, Rep]: Stack[ServiceFactory[Req, Rep]] = Stack.Leaf(Endpoint,
      new com.twitter.finagle.service.FailingFactory[Req, Rep](
        new IllegalArgumentException("Unterminated stack")))
  }
}
