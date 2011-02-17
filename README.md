# Finagle

## What is it?

Finagle is an library for building *asynchronous* RPC servers and clients in Java, Scala, or any JVM language. Built atop [Netty](http://www.jboss.org/netty), Finagle provides a rich set of tools that are protocol independent:

### Client Features

* Connection Pooling
* Load Balancing
* Failure Detection
* Failover/Retry
* Distributed Tracing (a la Dapper http://research.google.com/pubs/pub36356.html)
* Service Discovery (e.g., via Zookeeper)
* Rich Statistics
* Native OpenSSL bindings

### Server Features

* Backpressure (to defend against abusive clients)
* Service Registration (e.g., via Zookeeper)
* Native OpenSSL bindings

### Supported Protocols

* HTTP
* Thrift
* Memcached/Kestrel
* More to come!

Finagle is flexible enough to support a variety of RPC styles, including request-response, streaming, and pipelining (e.g., HTTP pipelining and Redis pipelining). It also makes it easy to work with stateful RPC styles (e.g., those requiring authentication and those that support transactions).

## How do I start?

Here is a simple HTTP server and client:

### Scala

    val service: Service[HttpRequest, HttpResponse] = new Service[HttpRequest, HttpResponse] {
      def apply(request: HttpRequest) = Future(new DefaultHttpResponse(HTTP_1_1, OK))
    }

    val address: SocketAddress = new InetSocketAddress(10000)

    val server: Server[HttpRequest, HttpResponse] = ServerBuilder()
      .codec(Http)
      .bindTo(address)
      .build(service)

    val client: Service[HttpRequest, HttpResponse] = ClientBuilder()
      .codec(Http)
      .hosts(address)
      .build()

    // Issue a request, get a response:
    val request: HttpRequest = new DefaultHttpRequest(HTTP_1_1, GET, "/")
    val responseFuture: Future[HttpResponse] = client(request)

### Java

Note that the variable `responseFuture` in this example is of type `Future[HttpResponse]`, which represents an asynchronous HTTP response (i.e., a response that will arrive sometime later). With a `Future` object, you can express your program in either a synchronous or asynchronous style: the program can either 1) block, awaiting the response, or 2) provide a callback to be invoked when a response is available. For example,

### Scala

     // (1) Synchronously await the response for up to 1 second, then print:
     println(responseFuture(1.second))

     // (2) Alternatively, when the response arrives, invoke the callback, then print:
     responseFuture foreach { response =>
       println(response)
     }

### Java

`Futures` allow the programmer to easily express a number of powerful idioms such as pipelining, scatter-gather, timeouts, and error handling. See the section "Using Futures" for more information.

## Services and Filters

In Finagle, RPC Servers are built out of `Services` and `Filters`. A `Service` is a simply a function that receives a request and returns a `Future` of a response. For example, here is a service that increments a number by one.

### Scala

    val plusOneService = new Service[Int, Int] {
      def apply(request: Int) = Future { request + 1 }                                  // (1)
    }

### Java

Note that `plusOneService` acts as if it were asynchronous despite that it is not doing any asynchronous work. It adheres to the `Service[Req, Rep]` contract by wrapping the synchronously computed response in a "constant" `Future` of type `Future[Int]`.

More sophisticated `Services` than `plusOneService` might make truly asynchronous calls (e.g., by making further RPC calls or by scheduling work in a queue), and so the return type of `Future[Rep]` is the more general.

Once you have defined your `Service`, it can be bound to a SocketAddress, thus becoming an RPC Server:

    ServerBuilder()
      .bindTo(address)
      .build(plusOneService)

However, an RPC Server must speak a specific protocol. One nice way to design an RPC Server is to decouple the protocol handling code from the implimentation of the business service. A `Filter` provides a easy way to do this.

Here is a `Filter` that adapts the `HttpRequest => HttpResponse` protocol to the `Int => Int` `plusOneService`:

### Scala

    val httpToIntFilter = new Filter[HttpRequest, HttpResponse, Int, Int] {              // (1)
      def apply(httpRequest: HttpRequest, intService: Service[Int, Int]) = {
        val intRequest = httpRequest.getContent.toString(CharsetUtil.UTF_8).toInt
        intService(intRequest) map { intResponse =>                                      // (2)
          val httpResponse = new DefaultHttpResponse(HTTP_1_1, OK)
          httpResponse.setContent(intResponse.toString.getBytes)
          httpResponse
        }
      }
    }

    val httpPlusOneService: Service[HttpRequest, HttpResponse] =
      httpToIntFilter.andThen(plusOneService)                                            // (3)

### Java

This example illustrates three important concepts:

1. A `Filter` wraps a `Service` and (potentially) converts the input and output types of the service to other types. Here `Int => Int` is mapped to `HttpRequest => HttpResponse`.
1. The result of a `Future[A]` computation can be converted to a `Future[B]` computation by calling the `map` function. This function is applied asynchronously and is analogous to the `map` function on sequences used in many programming languages. See the section "Using Futures" for more information.
1. A `Filter` is wrapped around a `Service` by calling the `andThen` function. Any number of `Filters` can be composed using `andThen`, as we will see shortly.

Let's consider a more involved example. Often it is nice to isolate distinct phases of your application into a pipeline, and `Filters` provide a great way to accomplish this. In order to prevent `Integer Overflow` errors, our `Service` will check that the request is `< 2**32 - 1`:

### Scala

    val preventOverflowFilter = new SimpleFilter[Int, Int] {                             // (1)
      def apply(request: Int, continue: Service[Int, Int]) =
        if (request < Int.MaxValue)
          continue(request)
        else
          Future.exception(new OverflowException)                                        // (2)
    }

### Notes

1. A `SimpleFilter` is a kind of `Filter` that does not convert the request and response types. It saves a little bit of typing.
1. An exception can be returned asynchronously by calling `Future.exception`. See the section "Using Futures" for more information.

Another `Filter` typical of an RPC Service is authentication and authorization. Our `Service` wants to ensure that the user is authorized to perform addition:

### Scala

    val ensureAuthorizedFilter = new SimpleFilter[HttpRequest, HttpResponse] {
      def apply(request: HttpRequest, continue: Service[Int, Int]) =
        if (request.getHeader("Authorization") == "Basic ...")
          continue(request)
        else
          Future.exception(new UnauthorizedException)
    }


Finally, all of the `Filters` can be composed with our `Service in the following way:

    val myService =
      ensureAuthorizedFilter andThen
      httpToIntFilter        andThen
      preventOverflowFilter  andThen
      plusOneService

## Building a robust RPC client

Finagle makes it easy to build RPC clients with connection pooling, load balancing, logging, and statistics reporting:

### Scala

    val client = ClientBuilder()
        .codec(Http)
        .hosts("localhost:10000,localhost:10001,localhost:10003")
        .connectionTimeout(1.second)        // max time to spend establishing a TCP connection.
        .retries(2)                         // (1) per-request retries
        .reportTo(new OstrichStatsReceiver) // export host-level load data to ostrich
        .logger(Logger.getLogger("http"))
        .build()


This creates a load balanced HTTP client that balances requests among 3 (local) endpoints. The balancing strategy is to pick the endpoint with the least number of outstanding requests (this is similar to "least connections" in other load balancers).

### Notes

1. If retries are specified (using `retries(n: Int)`), Finagle will retry the request in the event of an error, up to the number of times specified. Finagle **does not assume your RPC service is Idempotent**. Retries occur only in the event of TCP-related `WriteExceptions`, where we are certain the RPC has not been transmitted to the remote server.

Once you have constructed a client, a request is issued like this:

### Scala

    val request: HttpRequest = new DefaultHttpRequest(HTTP_1_1, Get, "/")
    val futureResponse: Future[HttpResponse] = client(request)

### Java

    HttpRequest request = new DefaultHttpRequest(HTTP_1_1, Get, "/")
    Future<HttpResponse> futureResponse = client.apply(request)

### Timeouts

A robust way to use RPC clients is to have an upper-bound on how long to wait for a response to arrive. With `Futures`, there are two ways to do this: synchronously and asynchrously. 1) The synchronous technique is to block, waiting for a response to arrive, and throw an exception if it does not arrive in time. 2) The asynchronous way is to register a callback to handle the result if it arrives in time, and invoke another callback if it fails.

### Scala

    // (1) synchronous timeouts
    try {
      val response = futureResponse(1.second)
    } catch {
      case e: TimeoutException => ...
    }

    // (2) asynchronous timeouts require an (implicit) Timer object
    import com.twitter.finagle.util.Timer._

    futureResponse.within(1.second) respond {
      case Throw(t) => ...
      case Return(response) =>
        println("yay it worked: " + response)
    }

## Using Futures

Finagle uses `com.twitter.util.Futures` as the unifying abstraction for all asynchronous computation. A `Future` represents a computation that has not yet completed, and that can succeed or fail. The two most basic ways to use a `Future` is to 1) wait for the computation to return, or 2) register a callback to be invoked when the computation eventually succeeds or fails.

In the example below, we define a function `f` that takes an `Int` and returns a `Future[Int]`. It errors if given an odd number.

### Scala

    def f(a: Int): Future[Int] =
      if (a % 2 == 0)
        Future.value(a)
      else
        Future.exception(new OddNumberException)

    val myFuture: Future[Int] = f(2)

    // 1) Wait 1 second the for computation to return
    try {
      println(myFuture(1.second))
    } catch {
      case e: TimeoutException   => ...
      case e: OddNumberException => ...
    }

    // 2) Invoke a callback when the computation succeeds or fails
    myFuture respond {
      case Return(i) => println(i)
      case Throw(e) => ...
    }

### Java

`Futures` employ a powerful set of combinators including `map`, `flatMap`, and `foreach`.

## Streaming Protocols

Some incomplete API documentation is available:
See [scaladoc](http://twitter.github.com/finagle/).
