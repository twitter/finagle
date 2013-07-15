package com.twitter

/**
  * =Finagle Introduction=
  *
  * Start with [[com.twitter.finagle]]. Both clients and and servers are services [[com.twitter.finagle.Service]], which are typically created with the ClientBuilder [[com.twitter.finagle.builder.ClientBuilder]] and ServerBuilder [[com.twitter.finagle.builder.ServerBuilder]] class, respectively. 
  *
  * The following example shows how to build a simple HTTP server:
  *
  * {{{
  * val service: Service[HttpRequest, HttpResponse] = new Service[HttpRequest, HttpResponse] { 
  *   def apply(request: HttpRequest) = Future(new DefaultHttpResponse(HTTP_1_1, OK))          
  * }
  * 
  * val address: SocketAddress = new InetSocketAddress(10000)                                  
  *
  * val server: Server[HttpRequest, HttpResponse] = ServerBuilder()                            
  *   .codec(Http)         // HTTP codec, defined in Netty
  *   .bindTo(address)     // one or more hosts; localhost:10000 in this example
  *   .name("HttpServer")  // name of your service
  *   .build(service)      // build the server 
  * }}}
  *
  * The following example shows how to build a simple HTTP client, which handles asynchronous responses:
  *
  * {{{
  * val client: Service[HttpRequest, HttpResponse] = ClientBuilder()                           
  *    .codec(Http)             // HTTP codec, defined in Netty
  *    .hosts(address)          // one or more hosts; localhost:10000 in this example
  *    .hostConnectionLimit(1)  // 1 connection per host
  *    .build()                 // build the client 
  *
  * // Issue a request, get a response: 
  * val request: HttpRequest = new DefaultHttpRequest(HTTP_1_1, GET, "/")                      
  * val responseFuture: Future[HttpResponse] = client(request)           // response to request is a Future, which enables async execution
  *   onSuccess { response => println("Received response: " + response)  // Finagle calls onSuccess when the response becomes available
  *                                                                      // onFailure could be defined to catch exceptions                      
  * }
  * }}}
  *
  * For information about the Future class, which is essential for most RPC applications, see the [[http://twitter.github.com/util/util-core/target/site/doc/main/api/ scaladoc for Util]].
  *
  * For exceptions you can catch, see [[com.twitter.finagle.RequestException]] and [[com.twitter.finagle.ApiException]].
  *
  * For imports and more ideas of how to use Finagle, see the [[https://github.com/twitter/finagle/tree/master/finagle-example/src/main/scala/com/twitter/finagle/example examples]].
  *
  */
package object finagle
