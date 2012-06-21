Finagle is built using [sbt](http://code.google.com/p/simple-build-tool/wiki/RunningSbt). We've included a bootstrap script to ensure the correct version of sbt is used. To build:

	$ ./sbt test

# Versioning

After version 5.0, Finagle is only compiled against Scala 2.9.x, and sbt-style crossbuild paths are no longer used.

---

<a name="Top"></a>

# Finagle Developer Guide (December 15, 2011 Draft)

* <a href="#Quick Start">Quick Start</a>
  - <a href="#Simple HTTP Server">Simple HTTP Server</a>
  - <a href="#Simple HTTP Client">Simple HTTP Client</a>
  - <a href="#Simple Client and Server for Thrift">Simple Client and Server for Thrift</a>
* <a href="#Finagle Overview">Finagle Overview</a>
  - <a href="#Client Features">Client Features</a>
  - <a href="#Server Features">Server Features</a>
  - <a href="#Supported Protocols">Supported Protocols</a>
* <a href="#Architecture">Architecture</a>
  - <a href="#Future Objects">Future Objects</a>
  - <a href="#Service Objects">Service Objects</a>
  - <a href="#Filter Objects">Filter Objects</a>
  - <a href="#Codec Objects">Codec Objects</a>
  - <a href="#Servers">Servers</a>
  - <a href="#Clients">Clients</a>
  - <a href="#Threading Model">Threading Model</a>
  - <a href="#Starting and Stopping Services">Starting and Stopping Servers</a>
  - <a href="#Exception Handling">Exception Handling</a>
* <a href="#Finagle Projects and Packages">Finagle Projects and Packages</a>
* <a href="#Using Future Objects">Using Future Objects</a>
  - <a href="#Future Callbacks">Future Callbacks</a>
  - <a href="#Future Timeouts">Future Timeouts</a>
  - <a href="#Future Exceptions">Future Exceptions</a>
  - <a href="#Promises">Promises</a>
  - <a href="#Using Future map and flatMap Operations">Using Future map and flatMap Operations</a>
  - <a href="#Using Future in Scatter/Gather Patterns">Using Future in Scatter/Gather Patterns</a>
  - <a href="#Using Future Pools">Using Future Pools</a>
* <a href="#Creating a Service">Creating a Service</a>
* <a href="#Creating Simple Filters">Creating Filters</a>
* <a href="#Building a Robust Server">Building a Robust Server</a>
* <a href="#Building a Robust Client">Building a Robust Client</a>
* <a href="#Creating Filters to Transform Requests and Responses">Creating Filters to Transform Requests and Responses</a>
* <a href="#Using ServerSet Objects">Using ServerSet Objects</a>
* <a href="#Java Design Patterns for Finagle">Java Design Patterns for Finagle</a>
  - <a href="#Using Future Objects With Java">Using Future Objects With Java</a>
  - <a href="#Imperative Java Style">Imperative Java Style</a>
  - <a href="#Functional Java Style">Functional Java Style</a>
  - <a href="#Building a Server in Java">Building a Server in Java</a>
  - <a href="#Building a Client in Java">Building a Client in Java</a>
  - <a href="#Implementing a Pool for Blocking Operations in Java">Implementing a Pool for Blocking Operations in Java</a>
* <a href="#Additional Samples">Additional Samples</a>
* <a href="#API Reference Documentation">API Reference Documentation</a>

<a name="Quick Start"></a>

## Quick Start

Finagle is an asynchronous network stack for the JVM that you can use to build *asynchronous* Remote Procedure Call (RPC) clients and servers in Java, Scala, or any JVM-hosted language. Finagle provides a rich set of tools that are protocol independent.

The following Quick Start sections show how to implement simple RPC servers and clients in Scala and Java. The first example shows the creation a simple HTTP server and corresponding client. The second example shows the creation of a Thrift server and client. You can use these examples to get started quickly and have something that works in just a few lines of code. For a more detailed description of Finagle and its features, start with <a href="#Finagle Overview">Finagle Overview</a> and come back to Quick Start later.

**Note:** The examples in this section include both Scala and Java implementations. Other sections show only Scala examples. For more information about Java, see <a href="#Java Design Patterns for Finagle">Java Design Patterns for Finagle</a>.

[Top](#Top)

<a name="Simple HTTP Server"></a>

### Simple HTTP Server

Consider a very simple implementation of an HTTP server and client in which clients make HTTP GET requests and the server responds to each one with an HTTP 200 OK response.

The following server, which is shown in both Scala and Java, responds to a client's HTTP request with an HTTP 200 OK response:

##### Scala HTTP Server Implementation

    val service: Service[HttpRequest, HttpResponse] = new Service[HttpRequest, HttpResponse] { // 1
      def apply(request: HttpRequest) = Future(new DefaultHttpResponse(HTTP_1_1, OK))          // 2
    }

    val address: SocketAddress = new InetSocketAddress(10000)                                  // 3

    val server: Server[HttpRequest, HttpResponse] = ServerBuilder()                            // 4
      .codec(Http)
      .bindTo(address)
      .name("HttpServer")
      .build(service)

##### Java HTTP Server Implementation

    Service<HttpRequest, HttpResponse> service = new Service<HttpRequest, HttpResponse>() {    // 1
      public Future<HttpResponse> apply(HttpRequest request) {
        return Future.value(                                                                   // 2
	        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
      }
    };

    ServerBuilder.safeBuild(service, ServerBuilder.get()                                       // 4
      .codec(Http.get())
      .name("HttpServer")
      .bindTo(new InetSocketAddress("localhost", 10000)));                                     // 3


##### HTTP Server Code Annotations

1. Create a new Service that handles HTTP requests and responses.
2. For each request, respond asynchronously with an HTTP 200 OK response. A Future instance represents an asynchronous operation that may be performed later.
3. Specify the socket addresses on which your server responds; in this case, on port 10000 of localhost.
4. Build a server that responds to HTTP requests on the socket and associate it with your service. In this case, the Server builder specifies
  - an HTTP codec, which ensures that only valid HTTP requests are received by the server
  - the host socket that listens for requests
  - the association between the server and the service, which is specified by `.build` in Scala and the first argument to `safeBuild` in Java
  - the name of the service

**Note:** For more information about the Java implementation, see <a href="#Java Design Patterns for Finagle">Java Design Patterns for Finagle</a>.

[Top](#Top)

<a name="Simple HTTP Client"></a>

### Simple HTTP Client

The client, which is shown in both Scala and Java, connects to the server, and issues a simple HTTP GET request:

##### Scala HTTP Client Implementation

    val client: Service[HttpRequest, HttpResponse] = ClientBuilder()                           // 1
      .codec(Http)
      .hosts(address)
      .hostConnectionLimit(1)
      .build()

    // Issue a request, get a response:
    val request: HttpRequest = new DefaultHttpRequest(HTTP_1_1, GET, "/")                      // 2
    val responseFuture: Future[HttpResponse] = client(request)                                 // 3
      onSuccess { response => println("Received response: " + response)                        // 4
	  }

##### Java HTTP Client Implementation

    Service<HttpRequest, HttpResponse> client = ClientBuilder.safeBuild(ClientBuilder.get()    // 1
      .codec(Http.get())
      .hosts("localhost:10000")
      .hostConnectionLimit(1));

    // Issue a request, get a response:
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");   // 2
    client.apply(request).addEventListener(new FutureEventListener<HttpResponse>() {           // 3
      public void onSuccess(HttpResponse response) {                                           // 4
        System.out.println("received response: " + response);
      }
      public void onFailure(Throwable cause) {
        System.out.println("failed with cause: " + cause);
      }
    });

##### HTTP Client Code Annotations

1. Build a client that sends an HTTP request to the host identified by its socket address. In this case, the Client builder specifies
  - an HTTP request filter, which ensures that only valid HTTP requests are sent to the server
  - a list of the server's hosts that can process requests
  - maximum number of connections from the client to the host
  - to build this client service
2. Create an HTTP GET request.
3. Make the request to the host identified in your client.
4. Specify a callback, `onSuccess`, that Finagle executes when the response arrives.

**Note:** Although the example shows building the client and execution of the built client on the same thread, you should build your clients only once and execute them separately. There is no requirement to maintain a 1:1 relationship between building a client and executing a client.

[Top](#Top)

<a name="Simple Client and Server for Thrift"></a>

### Simple Client and Server for Thrift

Apache Thrift is a binary communication protocol that defines available methods using an interface definition language (IDL). Consider the following Thrift IDL definition for a `Hello` service that defines only one method, `hi`:

    service Hello {
      string hi();
    }

#### Simple Thrift Server

In this Finagle example, the `ThriftServer` object implements the `Hello` service defined using the Thrift IDL.

##### Scala Thrift Server Implementation

    object ThriftServer {
      def main(args: Array[String]) {
        // Implement the Thrift Interface
        val processor = new Hello.ServiceIface {                                 // 1
        def hi() = Future.value("hi")                                            // 2
      }

      val service = new Hello.Service(processor, new TBinaryProtocol.Factory())  // 3

      val server: Server = ServerBuilder()                                       // 4
        .name("HelloService")
        .bindTo(new InetSocketAddress(8080))
        .codec(ThriftServerFramedCodec())
        .build(service)
      }
    }

##### Java Thrift Server Implementation

    Hello.ServiceIface processor = new Hello.ServiceIface() {                    // 1
    public Future<String> hi() {                                                 // 2
      return Future.value("hi");
      }
    }

    ServerBuilder.safeBuild(                                                     // 4
      new Hello.Service(processor, new TBinaryProtocol.Factory()),               // 3
      ServerBuilder.get()
        .name("HelloService")
        .codec(ThriftServerFramedCodec.get())
     // .codec(ThriftServerFramedCodecFactory$.MODULE$) previously
        .bindTo(new InetSocketAddress(8080)));

##### Thrift Server Code Annotations

1. Create a Thrift processor that implements the Thrift service interface, which is `Hello` in this example.
2. Implement the service interface. In this case, the only method in the interface is `hi`, which only returns the string `"hi"`. The returned value must be a `Future` to conform the signature of a Finagle `Service`. (In a more robust example, the Thrift service might perform asynchronous communication.)
3. Create an adapter from the Thrift processor to a Finagle service. In this case, the `Hello` Thrift service uses `TBinaryProtocol` as the Thrift protocol.
4. Build a server that responds to Thrift requests on the socket and associate it with your service. In this case, the Server builder specifies
  - the name of the service
  - the host addresses that can receive requests
  - the Finagle-provided `ThriftServerFramedCodec` codec, which ensures that only valid Thrift requests are received by the server
  - the association between the server and the service

#### Simple Thrift Client

In this Finagle example, the `ThriftClient` object creates a Finagle client that executes the methods defined in the `Hello` Thrift service.

##### Scala Thrift Client Implementation

    object ThriftClient {
      def main(args: Array[String]) {
        // Create a raw Thrift client service. This implements the
        // ThriftClientRequest => Future[Array[Byte]] interface.
        val service: Service[ThriftClientRequest, Array[Byte]] = ClientBuilder()               // 1
          .hosts(new InetSocketAddress(8080))
          .codec(ThriftClientFramedCodec())
          .hostConnectionLimit(1)
          .build()

        // Wrap the raw Thrift service in a Client decorator. The client provides
        // a convenient procedural interface for accessing the Thrift server.
        val client = new Hello.ServiceToClient(service, new TBinaryProtocol.Factory())         // 2

        client.hi() onSuccess { response =>                                                    // 3
          println("Received response: " + response)
        } ensure {
          service.release()                                                                    // 4
        }
      }
    }

##### Java Thrift Client Implementation

    Service<ThriftClientRequest, byte[]> client = ClientBuilder.safeBuild(ClientBuilder.get()  // 1
      .hosts(new InetSocketAddress(8080))
      .codec(new ThriftClientFramedCodecFactory())
      .hostConnectionLimit(1));

    Hello.ServiceIface client =
      new Hello.ServiceToClient(client, new TBinaryProtocol.Factory());                        // 2

    client.hi().addEventListener(new FutureEventListener<String>() {
      public void onSuccess(String s) {                                                        // 3
        System.out.println(s);
      }

      public void onFailure(Throwable t) {
        System.out.println("Exception! ", t.toString());
      }
    });

##### Thrift Client Code Annotation

1. Build a client that sends a Thrift protocol-based request to the host identified by its socket address. In this case, the Client builder specifies
	- the host addresses that can receive requests
	- the Finagle-provided `ThriftServerFramedCodec` codec, which ensures that only valid Thrift requests are received by the server
	- to build this client service
2. Make a remote procedure call to the `Hello` Thrift service's `Hi` method. This returns a `Future` that represents the eventual arrival of a response.
3. When the response arrives, the `onSuccess` callback executes to print the result.
4. Release resources acquired by the client.

[Top](#Top)

<a name="Finagle Overview"></a>

## Finagle Overview

Use the Finagle library to implement asynchronous Remote Procedure Call (RPC) clients and servers. Finagle is flexible enough to support a variety of RPC styles, including request-response, streaming, and pipelining; for example, HTTP pipelining and Redis pipelining. It also makes it easy to work with stateful RPC styles; for example, RPCs that require authentication and those that support transactions.

[Top](#Top)

<a name="Client Features"></a>

### Client Features

* Connection Pooling
* Load Balancing
* Failure Detection
* Failover/Retry
* Distributed Tracing (a la [Dapper](http://research.google.com/pubs/pub36356.html))
* Service Discovery (e.g., via Zookeeper)
* Rich Statistics
* Native OpenSSL Bindings

[Top](#Top)

<a name="Server Features"></a>

### Server Features

* Backpressure (to defend against abusive clients)
* Service Registration (e.g., via Zookeeper)
* Distributed Tracing
* Native OpenSSL bindings

[Top](#Top)

<a name="Supported Protocols"></a>

### Supported Protocols

* HTTP
* HTTP streaming (Comet)
* Thrift
* Memcached/Kestrel
* More to come!

[Top](#Top)

<a name="Architecture"></a>

## Architecture

Finagle extends the stream-oriented [Netty](http://www.jboss.org/netty) model to provide asynchronous requests and responses for remote procedure calls (RPC). Internally, Finagle manages a service stack to track outstanding requests, responses, and the events related to them. Finagle uses a Netty pipeline to manage connections between the streams underlying request and response messages. The following diagram shows the relationship between your RPC client or server, Finagle, Netty, and Java libraries:

![Relationship between your RPC client or server, Finagle, Netty, and Java Libraries (doc/FinagleRelationship.png)](https://github.com/twitter/finagle/raw/master/doc/FinagleRelationship.png)

Finagle manages a [Netty pipeline](http://docs.jboss.org/netty/3.2/api/org/jboss/netty/channel/ChannelPipeline.html) for servers built on Finagle RPC services. Netty itself is built on the Java [NIO](http://download.oracle.com/javase/1.5.0/docs/api/java/nio/channels/package-summary.html#package_description) library, which supports asynchronous IO. While an understanding of Netty or NIO might be useful, you can use Finagle without this background information.

Finagle objects are the building blocks of RPC clients and servers:

- <a href="#Future Objects">Future objects</a> enable asynchronous operations required by a service
- <a href="#Service Objects">Service objects</a> perform the work associated with a remote procedure call
- <a href="#Filter Objects">Filter objects</a> enable you to transform data or act on messages before or after the data or messages are processed by a service
- <a href="#Codec Objects">Codec objects</a> decode messages in a specific protocol before they are handled by a service and encode messages before they are transported to a client or server.

You combine these objects to create:

- <a href="#Servers">Servers</a>
- <a href="#Clients">Clients</a>

Finagle provides a `ServerBuilder` and a `ClientBuilder` object, which enable you to configure <a href="#Servers">servers</a> and <a href="#Clients">clients</a>, respectively.

[Top](#Top)

<a name="Future Objects"></a>

### Future Objects

In Finagle, `Future` objects are the unifying abstraction for all asynchronous computation. A `Future` represents a computation that has not yet completed, which can either succeed or fail. The two most basic ways to use a `Future` are to

* block and wait for the computation to return
* register a callback to be invoked when the computation eventually succeeds or fails

For more information about `Future` objects, see <a href="#Using Future Objects">Using Future Objects</a>.

<!-- Future has methods, such as `times`, `parallel`, and `whileDo` that implement advanced control-flow patterns. -->

[Top](#Top)

<a name="Service Objects"></a>

### Service Objects

A `Service` is simply a function that receives a request and returns a `Future` object as a response. You extend the abstract `Service` class to implement your service; specifically, you must define an `apply` method that transforms the request into the future response.

[Top](#Top)

<a name="Filter Objects"></a>

### Filter Objects

It is useful to isolate distinct phases of your application into a pipeline. For example, you may need to handle exceptions, authorization, and other phases before your service responds to a request. A `Filter` provides an easy way to decouple the protocol handling code from the implementation of the business rules. A `Filter` wraps a `Service` and, potentially, converts the input and output types of the service to other types. For an example of a filter, see <a href="#Creating Filters to Transform Requests and Responses">Creating Filters to Transform Requests and Responses</a>.

A `SimpleFilter` is a kind of `Filter` that does not convert the request and response types. For an example of a simple filter, see <a href="#Creating Filters">Creating Filters</a>.

[Top](#Top)

<a name="Codec Objects"></a>

### Codec Objects

A `Codec` object encodes and decodes _wire_ protocols, such as HTTP. You can use Finagle-provided `Codec` objects for encoding and decoding the Thrift, HTTP, memcache, Kestrel, HTTP chunked streaming (ala Twitter Streaming) protocols. You can also extend the `CodecFactory` class to implement encoding and decoding of other protocols.

[Top](#Top)

<a name="Servers"></a>

### Servers

In Finagle, RPC servers are built out of a `Service` and zero or more `Filter` objects. You apply filters to the service request after which you execute the service itself:

![Relationship between a service and filters (doc/Filters.png)](https://github.com/twitter/finagle/raw/master/doc/Filters.png)

Typically, you use a `ServerBuilder` to create your server. A `ServerBuilder` enables you to specify the following general attributes:

<table>
<thead>
<tr>
<th>Attribute</th>
<th>Description</th>
<th>Default Value</th>
</tr>
</thead>
<tr>
<td>codec</td>
<td>Object to handle encoding and decoding of the service's request/response protocol</td>
<td><I>None</I></td>
</tr>
<tr>
<td>statsReceiver</td>
<td>Statistics receiver object, which enables logging of important events and statistics</td>
<td><I>None</I></td>
</tr>
<tr>
<td>name</td>
<td>Name of the service</td>
<td><I>None</I></td>
</tr>
<tr>
<td>bindTo</td>
<td>The IP host:port pairs on which to listen for requests; <CODE>localhost</CODE> is assumed if the host is not specified</td>
<td><I>None</I></td>
</tr>
<tr>
<td>logger</td>
<td>Logger object</td>
<td><I>None</I></td>
</tr>
</tbody>
</table>

You can specify the following attributes to handle fault tolerance and manage clients:

<table>
<thead>
<tr>
<th>Attribute</th>
<th>Description</th>
<th>Default Value</th>
</tr>
</thead>
<tr>
<td>maxConcurrentRequests</td>
<td>Maximum number of requests that can be handled concurrently by the server</td>
<td><I>None</I></td>
</tr>
<tr>
<td>hostConnectionMaxIdleTime</td>
<td>Maximum time that this server can be idle before the connection is closed</td>
<td><I>None</I></td>
</tr>
<tr>
<td>hostConnectionMaxLifeTime</td>
<td>Maximum time that this server can be connected before the connection is closed</td>
<td><I>None</I></td>
</tr>
<tr>
<td>requestTimeout</td>
<td>Maximum time to complete a request</td>
<td><I>None</I></td>
</tr>
<tr>
<td>readTimeout</td>
<td>Maximum time to wait for the first byte to be read</td>
<td><I>None</I></td>
</tr>
<tr>
<td>writeCompletionTimeout</td>
<td>Maximum time to wait for notification of write completion from a client</td>
<td><I>None</I></td>
</tr>
</tbody>
</table>

You can specify the following attributes to manage TCP connections:

<table>
<thead>
<tr>
<th>Attribute</th>
<th>Description</th>
<th>Default Value</th>
</tr>
</thead>
<tr>
<td>sendBufferSize</td>
<td>Requested TCP buffer size for responses</td>
<td><I>None</I></td>
</tr>
<tr>
<td>recvBufferSize</td>
<td>Actual TCP buffer size for requests</td>
<td><I>None</I></td>
</tr>
</tbody>
</table>

You can also specify these attributes:

<table>
<thead>
<tr>
<th>Attribute</th>
<th>Description</th>
<th>Default Value</th>
</tr>
</thead>
<tr>
<td>tls</td>
<td>The kind of transport layer security</td>
<td><I>None</I></td>
</tr>
<tr>
<td>channelFactory</td>
<td>Channel service factory object</td>
<td><I>None</I></td>
</tr>
<tr>
<td>traceReceiver</td>
<td>Trace receiver object</td>
<td>new <CODE>NullTraceReceiver</CODE> object</td>
</tr>
</tbody>
</table>

Once you have defined your `Service`, it can be bound to an IP socket address, thus becoming an RPC server.

[Top](#Top)

<a name="Clients"></a>

### Clients

Finagle makes it easy to build RPC clients with connection pooling, load balancing, logging, and statistics reporting. The balancing strategy is to pick the endpoint with the least number of outstanding requests, which is similar to _least connections_ in other load balancers. The load-balancer deliberately introduces jitter to avoid synchronicity (and thundering herds) in a distributed system.

Your code should separate building the client from invocation of the client. A client, once built, can be used with _lazy binding_, saving the resources required to build a client. Note: The examples, which show the creation of the client and its first execution together, represent the first-execution scenario. Typically, subsequent execution of the client does not require rebuilding.

Finagle will retry the request in the event of an error, up to the number of times specified; however, Finagle **does not assume your RPC service is Idempotent**. Retries occur only when the request is known to be idempotent, such as in the event of TCP-related `WriteException` errors, for which the RPC has not been transmitted to the remote server.

A robust way to use RPC clients is to have an upper-bound on how long to wait for a response to arrive. With `Future` objects, you can

* block, waiting for a response to arrive and throw an exception if it does not arrive in time.
* register a callback to handle the result if it arrives in time, and register another callback to invoke if the result does not arrive in time

A client is a `Service` and can be wrapped by `Filter` objects. Typically, you call `ClientBuilder` to create your client service. `ClientBuilder` enables you to specify the following general attributes:

<table>
<thead>
<tr>
<th>Attribute</th>
<th>Description</th>
<th>Default Value</th>
</tr>
</thead>
<tr>
<td>name</td>
<td>Name of the service</td>
<td><I>None</I></td>
</tr>
<tr>
<td>codec</td>
<td>Object to handle encoding and decoding of the service's request/response protocol</td>
<td><I>None</I></td>
</tr>
<tr>
<td>statsReceiver</td>
<td>Statistics receiver object, which enables logging of important events and statistics</td>
<td><I>None</I></td>
</tr>
<tr>
<td>loadStatistics</td>
<td>How often to load statistics from the server</td>
<td><B>(60, 10.seconds)</B></td>
</tr>
<tr>
<td>logger</td>
<td>A <CODE>Logger</CODE> object with which to log Finagle messages</td>
<td><I>None</I></td>
</tr>
<tr>
<td>retries</td>
<td>Number of retries per request (only applies to recoverable errors)</td>
<td><I>None</I></td>
</tr>
</tbody>
</table>

You can specify the following attributes to manage the host connection:

<table>
<thead>
<tr>
<th>Attribute</th>
<th>Description</th>
<th>Default Value</th>
</tr>
</thead>
<tr>
<td>connectionTimeout</td>
<td>Time allowed to establish a connection</td>
<td><B>10.milliseconds</B></td>
</tr>
<tr>
<td>requestTimeout</td>
<td>Request timeout</td>
<td><I>None</I>, meaning it waits forever</td>
</tr>
<tr>
<td>hostConnectionLimit</td>
<td>Number of connections allowed from this client to the host</td>
<td><I>None</I></td>
</tr>
<tr>
<td>hostConnectionCoresize</td>
<td>Host connection's cache allocation</td>
<td><I>None</I></td>
</tr>
<tr>
<td>hostConnectionIdleTime</td>
<td></td>
<td><I>None</I></td>
</tr>
</tr>
<tr>
<td>hostConnectionMaxWaiters</td>
<td>The maximum number of queued requests awaiting a connection</td>
<td><I>None</I></td>
</tr>
<tr>
<td>hostConnectionMaxIdleTime</td>
<td>Maximum time that the client can be idle until the connection is closed</td>
<td><I>None</I></td>
</tr>
<tr>
<td>hostConnectionMaxLifeTime</td>
<td>Maximum time that client can be connected before the connection is closed</td>
<td><I>None</I></td>
</tr>
</tbody>
</table>

You can specify the following attributes to manage TCP connections:

<table>
<thead>
<tr>
<th>Attribute</th>
<th>Description</th>
<th>Default Value</th>
</tr>
</thead>
<tr>
<td>sendBufferSize</td>
<td>Requested TCP buffer size for responses</td>
<td><I>None</I></td>
</tr>
<tr>
<td>recvBufferSize</td>
<td>Actual TCP buffer size for requests</td>
<td><I>None</I></td>
</tr>
</tbody>
</table>

You can also specify these attributes:

<table>
<tr>
<th>Attribute</th>
<th>Description</th>
<th>Default Value</th>
</tr>
<tr>
<td>cluster</td>
<td>The cluster connections associated with the client</td>
<td><I>None</I></td>
</tr>
<tr>
<td>channelFactory</td>
<td>Channel factory associated with this client</td>
<td><I>None</I></td>
</tr>
<tr>
<td>tls</td>
<td>The kind of transport layer security</td>
<td><I>None</I></td>
</tr>
</table>

If you are using _stateful protocols_, such as those used for transaction processing or authentication, you should call `buildFactory`, which creates a `ServiceFactory` to support stateful connections.

[Top](#Top)

<a name="Threading Model"></a>

### Threading Model

The Finagle threading model requires that you avoid blocking operations in the Finagle event loop. Finagle-provided methods do not block; however, you could inadvertently implement a client, service or a `Future` callback that blocks.

Blocking events include but are not limited to

* network calls
* system calls
* database calls

Note: You do not need to be concerned with long-running or CPU intensive operations if they do not block. Examples of these operations include image processing operations, public key cryptography, or anything that might take a non-trivial amount of clock time to perform. Only operations that block in Finagle are of concern. Because Finagle and its event loop use a relatively low number of threads, blocked threads can cause performance issues.

Consider the following diagram, which shows how a client uses the Finagle event loop:

![Relationship between your threads and Finagle (doc/ThreadEx.png)](https://github.com/twitter/finagle/raw/master/doc/ThreadEx.png)

Your threads, which are shown on the left, are allowed to block. When you call a Finagle method or Finagle calls a method for you, it dispatches execution of these methods to its internal threads. Thus, the Finagle event loop and its threads cannot block without degrading the performance of other clients and servers that use the same Finagle instance.

In complex RPC operations, it may be necessary to perform blocking operations. In these cases, you must set up your own thread pool and use `Future` or `FuturePool` objects to execute the blocking operation on your own thread. Consider the following diagram:

![Handling operations that block (doc/ThreadExNonBlockingServer.png)](https://github.com/twitter/finagle/raw/master/doc/ThreadExNonBlockingServer.png)

In this example, you can use a `FuturePool` object to provide threads for blocking operations outside of Finagle. Finagle can then dispatch the blocking operation to your thread. For more information about `FuturePool` objects, see <a href="#Using Future Pools">Using Future Pools</a>.

[Top](#Top)

<a name="Starting and Stopping Servers"></a>

### Starting and Stopping Servers

A server automatically starts when you call `build` on the server after assigning the IP address on which it runs. To stop a server, call its `close` method. The server will immediately stop accepting requests; however, the server will continue to process outstanding requests until all have been handled or until a specific duration has elapsed. You specify the duration when you call `close`. In this way, the server is allowed to drain out outstanding requests but will not run indefinitely. You are responsible for releasing all resources when the server is no longer needed.

[Top](#Top)

<a name="Finagle Projects and Packages"></a>

## Finagle Projects and Packages

The `Core` project contains the execution framework, Finagle classes, and supporting classes, whose objects are only of use within Finagle. The `Core` project includes the following packages:

* `builder` - contains `ClientBuilder`, `ServerBuilder`
* `channel`
* `http`
* `loadbalancer`
* `pool`
* `service`
* `stats`
* `tracing`
* `util`

It also contains packages to support remote procedure calls over Kestrel, Thrift, streams, clusters, and provides statistics collection (Ostrich).

The `Util` project contains classes, such as `Future`, which are both generally useful and specifically useful to Finagle.

[Top](#Top)

<a name="Using Future Objects"></a>

## Using Future Objects

In the simplest case, you can use `Future` to block for a request to complete. Consider an example that blocks for an HTTP GET request:

	// Issue a request, get a response:
	val request: HttpRequest = new DefaultHttpRequest(HTTP_1_1, GET, "/")
	val responseFuture: Future[HttpResponse] = client(request)

In this example, a client issuing the request will wait forever for a response unless you specified a value for the `requestTimeout` attribute when you built the <a href="#Clients">client</a>.

Consider another example:

	val responseFuture: Future[String] = executor.schedule(job)

In this example, the value of `responseFuture` is not available until after the scheduled job has finished executing and the caller will block until `responseFuture` has a value.

**Note:** For examples of using Finagle `Future` objects in Java, see <a href="#Using Future Objects With Java">Using Future Objects With Java</a>.

[Top](#Top)

<a name="Future Callbacks"></a>

### Future Callbacks

In cases where you want to continue execution immediately, you can specify a callback. The callback is identified by the `onSuccess` keyword:

    val request: HttpRequest = new DefaultHttpRequest(HTTP_1_1, GET, "/")
    val responseFuture: Future[HttpResponse] = client(request)
    responseFuture onSuccess { responseFuture =>
      println(responseFuture)
    }

[Top](#Top)

<a name="Future Timeouts"></a>

### Future Timeouts

In cases where you want to continue execution after some amount of elapsed time, you can specify the length of time to wait in the `Future` object. The following example waits 1 second before displaying the value of the response:

    val request: HttpRequest = new DefaultHttpRequest(HTTP_1_1, GET, "/")
    val responseFuture: Future[HttpResponse] = client(request)
    println(responseFuture(1.second))

In the above example, you do not know whether the response timed out before the request was satisfied. To determine what kind of response you actually received, you can provide two callbacks, one to handle `onSuccess` conditions and one for `onFailure` conditions. You  use the `within` method of `Future` to specify how long to wait for the response. Finagle also creates a `Timer` thread on which to wait until one of the conditions are satisfied. Consider the following example:

    import com.twitter.finagle.util.Timer._
    ...
    val request: HttpRequest = new DefaultHttpRequest(HTTP_1_1, GET, "/")
    val responseFuture: Future[HttpResponse] = client(request)
    responseFuture.within(1.second) onSuccess { response =>
      println("responseFuture)
    } onFailure {
      case e: TimeoutException => ...
    }

If a timeout occurs, Finagle takes the `onFailure` path. You can use a `TimeoutException` object to display a message or take other actions.

[Top](#Top)

<a name="Future Exceptions"></a>

### Future Exceptions

To set up an exception, specify the action in a `try` block and handle failures in a `catch` block. Consider an example that handles `Future` timeouts as an exception:

    val request: HttpRequest = new DefaultHttpRequest(HTTP_1_1, GET, "/")
    val responseFuture: Future[HttpResponse] = client(request)
    try {
      println(responseFuture(1.second))
    } catch {
      case e: TimeoutException => ...
    }

In this example, after 1 second, either the HTTP response is displayed or the `TimeoutException` is thrown.

[Top](#Top)

<a name="Promises"></a>

### Promises

`Promise` is a subclass of `Future`. Although a `Future`  can only be read, a `Promise` can be both read and written.
Usually a producer makes a `Promise` and casts it to a `Future` before giving it to the consumer. The following example shows how this might be useful in the case where you intend to make a `Future` service but need to anticipate errors:

    def make() = {
    ...
    val promise = new Promise[Service[Req, Rep]]
    ... {
      case Ok(myObject) =>
        ...
        promise() = myConfiguredObject
      case Error(cause) =>
        promise() = Throw(new ...Exception(cause))
      case Cancelled =>
        promise() = Throw(new WriteException(new ...Exception))
      }
      promise
    }
    
You are discouraged from creating your own Promises. Instead, where possible, use `Future` combinators to compose actions (discussed next).

[Top](#Top)

<a name="Using Future map and flatMap Operations"></a>

### Using Future map and flatMap Operations

In addition to waiting for results to return, `Future` can be transformed in interesting ways. For instance, it is possible to convert a `Future[String]` to a `Future[Int]` by using `map`:

    val stringFuture: Future[String] = Future("1")
    val intFuture: Future[Int] = stringFuture map (_.toInt)

Similar to `map`, you can use `flatMap` to easily _pipeline_ a sequence of `Futures`:

    val authenticateUser: Future[User] = User.authenticate(email, password)
    val lookupTweets: Future[Seq[Tweet]] = authenticateUser flatMap { user =>
      Tweet.findAllByUser(user)
    }

In this example, `Tweet.findAllByUser(user)` is a function of type `User => Future[Seq[Tweet]]`.

<!--

[Top](#Top)

<a name="Using Future handle and rescue Operations"></a>

### Using Future handle and rescue Operations

// TODO

-->

[Top](#Top)

<a name="Using Future in Scatter/Gather Patterns"></a>

### Using Future in Scatter/Gather Patterns

For scatter/gather patterns, the challenge is to issue a series of requests in parallel and wait for all of them to arrive. To wait for a sequence of `Future` objects to return, you can define a sequence to hold the objects and use the `Future.collect` method to wait for them, as follows:

    val myFutures: Seq[Future[Int]] = ...
    val waitTillAllComplete: Future[Seq[Int]] = Future.collect(myFutures)

A more complex variation of scatter/gather pattern is to perform a sequence of asynchronous operations and harvest only those that return within a certain time, ignoring those that don't return within the specified time. For example, you might want to issue a set of parallel requests to _N_ partitions of a search index; those that don't return in time are assumed to be empty. The following example allows 1 second for the query to return:

    import com.twitter.finagle.util.Timer._

    val results: Seq[Future[Result]] = partitions.map { partition =>
      partition.get(query).within(1.second) handle {
        case _: TimeoutException => EmptyResult
      }
    }
    val allResults: Future[Seq[Result]] = Future.collect(timedResults)

    allResults onSuccess { results =>
      println(results)
    }

[Top](#Top)

<a name="Using Future Pools"></a>

### Using Future Pools

A `FuturePool` object enables you to place a blocking operation on its own thread. In the following example, a service's `apply` method, which executes in the Finagle event loop, creates the `FuturePool` object and places the blocking operation on a thread associated with the `FuturePool` object. The `apply` method returns immediately without blocking.

    class ThriftFileReader extends Service[String, Array[Byte]] {
      val diskIoFuturePool = FuturePool(Executors.newFixedThreadPool(4))

      def apply(path: String) = {
        def blockingOperation = {
          scala.Source.fromFile(path) // potential to block
        }
        // give this blockingOperation to the future pool to execute
        diskIoFuturePool(blockingOperation)
        // returns immediately while the future pool executes the operation on a different thread
      }
    }

**Note:** For an example implementation of a thread pool in Java, see <a href="#Implementing a Pool for Blocking Operations in Java">Implementing a Pool for Blocking Operations in Java</a>.

[Top](#Top)

<a name="Creating a Service"></a>

## Creating a Service

The following example extends the `Service` class to respond to an HTTP request:

    class Respond extends Service[HttpRequest, HttpResponse] {
      def apply(request: HttpRequest) = {
        val response = new DefaultHttpResponse(HTTP_1_1, OK)
        response.setContent(copiedBuffer(myContent, UTF_8))
        Future.value(response)
      }
    }

[Top](#Top)

<a name="Creating Simple Filters"></a>

## Creating Simple Filters

The following example extends the `SimpleFilter` class to throw an exception if the HTTP authorization header contains a different value than the specified string:

    class Authorize extends SimpleFilter[HttpRequest, HttpResponse] {
      def apply(request: HttpRequest, continue: Service[HttpRequest, HttpResponse]) = {
        if ("shared secret" == request.getHeader("Authorization")) {
          continue(request)
        } else {
          Future.exception(new IllegalArgumentException("You don't know the secret"))
        }
      }
    }

The following example extends the `SimpleFilter`class to set the HTTP response code if an error occurs and return the error and stack trace in the response:

    class HandleExceptions extends SimpleFilter[HttpRequest, HttpResponse] {
      def apply(request: HttpRequest, service: Service[HttpRequest, HttpResponse]) = {
        service(request) handle { case error =>
          val statusCode = error match {
            case _: IllegalArgumentException =>
              FORBIDDEN
            case _ =>
              INTERNAL_SERVER_ERROR
            }

          val errorResponse = new DefaultHttpResponse(HTTP_1_1, statusCode)
          errorResponse.setContent(copiedBuffer(error.getStackTraceString, UTF_8))

          errorResponse
        }
      }
    }

For an example implementation using a `Filter` object, see <a href="#Creating Filters to Transform Requests and Responses">Creating Filters to Transform Requests and Responses</a>.

[Top](#Top)

<a name="Building a Robust Server"></a>

## Building a Robust Server

The following example encapsulates the filters and service in the previous examples and defines the execution order of the filters, followed by the service. The `ServerBuilder` object specifies the service that indicates the execution order along with the codec and IP address on which to bind the service:

    object HttpServer {
      class HandleExceptions extends SimpleFilter[HttpRequest, HttpResponse] {...}
      class Authorize extends SimpleFilter[HttpRequest, HttpResponse] {...}
      class Respond extends Service[HttpRequest, HttpResponse] {... }


      def main(args: Array[String]) {
        val handleExceptions = new HandleExceptions
        val authorize = new Authorize
        val respond = new Respond

      val myService: Service[HttpRequest, HttpResponse]
        = handleExceptions andThen authorize andThen respond

      val server: Server = ServerBuilder()
        .codec(Http)
        .bindTo(new InetSocketAddress(8080))
        .build(myService)
      }
    }

In this example, the `HandleExceptions` filter is executed before the `authorize` filter. All filters are executed before the service. The server is robust not because of its complexity; rather, it is robust because it uses filters to remove issues before the service executes.

[Top](#Top)

<a name="Building a Robust Client"></a>

## Building a Robust Client

A robust client has little to do with the lines of code (SLOC) that goes into it; rather, the robustness depends on how you configure the client and the testing you put into it. Consider the following HTTP client:

    val client = ClientBuilder()
      .codec(Http)
      .hosts("localhost:10000,localhost:10001,localhost:10003")
      .hostConnectionLimit(1)             // max number of connections at a time to a host
      .connectionTimeout(1.second)        // max time to spend establishing a TCP connection
      .retries(2)                         // (1) per-request retries
      .reportTo(new OstrichStatsReceiver) // export host-level load data to ostrich
      .logger(Logger.getLogger("http"))
      .build()

The `ClientBuilder` object creates and configures a load balanced HTTP client that balances requests among 3 (local) endpoints. The Finagle balancing strategy is to pick the endpoint with the least number of outstanding requests, which is similar to a *least connections* strategy in other load balancers. The Finagle load balancer deliberately introduces jitter to avoid synchronicity (and thundering herds) in a distributed system. It also supports failover.

The following examples show how to invoke this client from Scala and Java, respectively:

##### Scala Client Invocation

    val request: HttpRequest = new DefaultHttpRequest(HTTP_1_1, Get, "/")
    val futureResponse: Future[HttpResponse] = client(request)

##### Java Client Invocation

    HttpRequest request = new DefaultHttpRequest(HTTP_1_1, Get, "/")
    Future<HttpResponse> futureResponse = client.apply(request)

For information about using `Future` objects with Java, see <a href="#Using Future Objects With Java">Using Future Objects With Java</a>.

[Top](#Top)

<a name="Creating Filters to Transform Requests and Responses"></a>

## Creating Filters to Transform Requests and Responses

The following example extends the `Filter` class to authenticate requests. The request is transformed into an HTTP response before being handled by the `AuthResult` service. In this case, the `RequireAuthentication` filter does not transform the resulting HTTP response:

    class RequireAuthentication(val p: ...)
      extends Filter[Request, HttpResponse, AuthenticatedRequest, HttpResponse]
      {
        def apply(request: Request, service: Service[AuthenticatedRequest, HttpResponse]) = {
          p.authenticate(request) flatMap {
            case AuthResult(AuthResultCode.OK, Some(passport: OAuthPassport), _, _) =>
              service(AuthenticatedRequest(request, passport))
            case AuthResult(AuthResultCode.OK, Some(passport: SessionPassport), _, _) =>
              service(AuthenticatedRequest(request, passport))
            case ar: AuthResult =>
              Trace.record("Authentication failed with " + ar)
              Future.exception(new RequestUnauthenticated(ar.resultCode))
        }
      }
    }

In this example, the `flatMap` object enables pipelining of the requests.

[Top](#Top)

<!--

<a name="Creating Combinators to Distribute Processing Tasks"></a>

## Creating Combinators to Distribute Processing Tasks

// TODO

[Top](#Top)

-->

<a name="Using ServerSet Objects"></a>

## Using ServerSet Objects

`finagle-serversets` is an implementation of the Finagle Cluster interface using `com.twitter.com.zookeeper` [ServerSets](http://twitter.github.com/commons/apidocs/#com.twitter.common.zookeeper.ServerSet).

You can instantiate a `ServerSet` object as follows:

    val serverSet = new ServerSetImpl(zookeeperClient, "/twitter/services/...")
    val cluster = new ZookeeperServerSetCluster(serverSet)

Servers join a cluster, as in the following example:

    val serviceAddress = new InetSocketAddress(...)
    val server = ServerBuilder()
      .bindTo(serviceAddress)
      .build()

    cluster.join(serviceAddress)

A client can access a cluster, as follows:

    val client = ClientBuilder()
      .cluster(cluster)
      .hostConnectionLimit(1)
      .codec(new StringCodec)
      .build()

[Top](#Top)

<a name="Java Design Patterns for Finagle"></a>

## Java Design Patterns for Finagle

The implementations of RPC servers and clients in Java are similar to Scala implementations. You can write your Java services in the _imperative_ style, which is the traditional Java programming style, or you can write in the _functional_ style, which uses functions as objects and is the basis for Scala. Most differences between Java and Scala implementations are related to exception handling.  

[Top](#Top)

<a name="Using Future Objects With Java"></a>

### Using Future Objects With Java

A `Future` object in Java is defined as `Future<`_Type_`>`, as in the following example:

    Future<String> future = executor.schedule(job);

**Note:** The `Future` class is defined in `com.twitter.util.Future` and is not the same as the Java `Future` class.

You can explicitly call the `Future` object's `get` method to retrieve the contents of a `Future` object:

    // Wait indefinitely for result
    String result = future.get();

Calling `get` is the more common pattern because you can more easily perform exception handling. See <a href="#Handling Synchronous Responses With Exception Handling">Handling Synchronous Responses With Exception Handling</a> for more information.

You can alternatively call the `Future` object's `apply` method. Arguments to the `apply` method are passed as functions:

    // Wait up to 1 second for result
    String result = future.apply(Duration.apply(1, SECOND));

This technique is most appropriate when exception handling is not an issue.

[Top](#Top)

<a name="Imperative Java Style"></a>

### Imperative Java Style

The following example shows the _imperative_ style, which uses an event listener that responds to a change in the `Future` object and calls the appropriate method:

    Future<String> future = executor.schedule(job);
    future.addEventListener(
      new FutureEventListener<String>() {
        public void onSuccess(String value) {
          println(value);
        }
        public void onFailure(Throwable t) ...
      }
    )

[Top](#Top)

<a name="Functional Java Style"></a>

### Functional Java Style


The following example shows the _functional_ style, which is similar to the way in which you write Scala code:

    Future<String> future = executor.schedule(job);
      future.onSuccess( new Function<String, Void>() {
        public Void apply(String value) { System.out.println(value);
      } ).onFailure(...).ensure(...);

The following example shows the _functional_ style for the `map` method:

    Future<String> future = executor.schedule(job);
    Future<Integer> result = future.map(new Function<String, Integer>() {
      public Integer apply(String value) { return Integer.valueOf(value);
      }

[Top](#Top)

<a name="Building a Server in Java"></a>

### Building a Server in Java

When you create a server in Java, you have several options. You can create a server that processes requests synchronously or asynchronously. You must also choose an appropriate level of exception handling. In all cases, either a `Future` or an exception is returned.This section shows several techniques that are relevant for servers written in Java:

* <a href="#Server Imports">Server Imports</a>
* <a href="#Performing Synchronous Operations">Performing Synchronous Operations</a>
* <a href="#Performing Asynchronous Operations">Performing Asynchronous Operations</a>
* <a href="#Invoking the Server">Invoking the Server</a>

[Top](#Top)

<a name="Server Imports"></a>

#### Server Imports

As you write a server in Java, you will become familiar with the following packages and classes. Some `netty` classes are specifically related to HTTP. Most of the classes you will use are defined in the `com.twitter.finagle` and `com.twitter.util` packages.

    import java.net.InetSocketAddress;

    import org.jboss.netty.buffer.ChannelBuffers;
    import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
    import org.jboss.netty.handler.codec.http.HttpRequest;
    import org.jboss.netty.handler.codec.http.HttpResponse;
    import org.jboss.netty.handler.codec.http.HttpResponseStatus;
    import org.jboss.netty.handler.codec.http.HttpVersion;

    import com.twitter.finagle.Service;
    import com.twitter.finagle.builder.ServerBuilder;
    import com.twitter.finagle.http.Http;
    import com.twitter.util.Future;
    import com.twitter.util.FutureEventListener;
    import com.twitter.util.FutureTransformer;

[Top](#Top)

<a name="Performing Synchronous Operations"></a>

#### Performing Synchronous Operations

If your server can respond synchronously, you can use the following pattern to implement your service:

    public class HTTPServer extends Service<HttpRequest, HttpResponse> {
      public Future<HttpResponse> apply(HttpRequest request) {
      // If I can generate the response synchronously, then I just do this.
      try {
        HttpResponse response = processRequest(request);
        return Future.value(response);
      } catch (MyException e) {
        return Future.exception(e);
      }

In this example, the `try` `catch` block causes the server to either return a response or an exception.

[Top](#Top)

<a name="Performing Asynchronous Operations"></a>

#### Performing Asynchronous Operations

In Java, you can implement asynchronous operations by calling a `Future` object's `getContentAsync` method to obtain the content from an asynchronous request. The `Future` object's `transformedBy` method transforms the content of the `Future` object from one data type to another, with the help of a `FutureTransformer` object. You typically override the object's `map` method to perform the actual conversion. A `Throwable` object is provided when an exception occurs to communicate information about the kind of exception. The following example shows this pattern:

    public class HTTPServer extends Service<HttpRequest, HttpResponse> {
      public Future<HttpResponse> apply(HttpRequest request) {

        Future<String> contentFuture = getContentAsync(request);
        return contentFuture.transformedBy(new FutureTransformer<String, HttpResponse>() {
          @Override
          public HttpResponse map(String content) {
            HttpResponse httpResponse =
              new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
            httpResponse.setContent(ChannelBuffers.wrappedBuffer(content.getBytes()));
	          return httpResponse;
          }

          @Override
          public HttpResponse handle(Throwable throwable) {
            HttpResponse httpResponse =
              new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.SERVICE_UNAVAILABLE);
            httpResponse.setContent(ChannelBuffers.wrappedBuffer(throwable.toString().getBytes()));
            return httpResponse;
          }
        }
      });
    }

[Top](#Top)

<a name="Invoking the Server"></a>

#### Invoking the Server

The following example shows the instantiation and invocation of the server. Calling the `ServerBuilder`'s `safeBuild` method statically checks arguments to `ServerBuilder`, which prevents a runtime error if a required argument is missing:

      public static void main(String[] args) {
        ServerBuilder.safeBuild(new HTTPServer(),
                                ServerBuilder.get()
                                             .codec(Http.get())
                                             .name("HTTPServer")
                                             .bindTo(new InetSocketAddress("localhost", 8080)));

      }
    }


[Top](#Top)

<a name="Building a Client in Java"></a>

### Building a Client in Java

When you create a client in Java, you have several options. You can create a client that processes responses synchronously or asynchronously. You must also choose an appropriate level of exception handling. This section shows several techniques that are relevant for clients written in Java:

* <a href="#Client Imports">Client Imports</a>
* <a href="#Creating the Client">Creating the Client</a>
* <a href="#Handling Synchronous Responses">Handling Synchronous Responses</a>
* <a href="#Handling Synchronous Responses With Timeouts">Handling Synchronous Responses With Timeouts</a>
* <a href="#Handling Synchronous Responses With Exception Handling">Handling Synchronous Responses With Exception Handling</a>
* <a href="#Handling Asynchronous Responses">Handling Asynchronous Responses</a>

[Top](#Top)

<a name="Client Imports"></a>

#### Client Imports

As you write a client in Java, you will become familiar with the following packages and classes. Some `netty` classes are specifically related to HTTP. Most of the classes you will use are defined in the `com.twitter.finagle` and `com.twitter.util` packages.

    import java.net.InetSocketAddress;
    import java.util.concurrent.TimeUnit;

    import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
    import org.jboss.netty.handler.codec.http.HttpMethod;
    import org.jboss.netty.handler.codec.http.HttpRequest;
    import org.jboss.netty.handler.codec.http.HttpResponse;
    import org.jboss.netty.handler.codec.http.HttpVersion;

    import com.twitter.finagle.Service;
    import com.twitter.finagle.builder.ClientBuilder;
    import com.twitter.finagle.http.Http;
    import com.twitter.util.Duration;
    import com.twitter.util.FutureEventListener;
    import com.twitter.util.Throw;
    import com.twitter.util.Try;


[Top](#Top)

<a name="Creating the Client"></a>

#### Creating the Client

The following example shows the instantiation and invocation of a client. Calling the `ClientBuilder`'s `safeBuild` method statically checks arguments to `ClientBuilder`, which prevents a runtime error if a required argument is missing:

    public class HTTPClient {
      public static void main(String[] args) {
        Service<HttpRequest, HttpResponse> httpClient =
          ClientBuilder.safeBuild(
            ClientBuilder.get()
                         .codec(Http.get())
                         .hosts(new InetSocketAddress(8080))
                         .hostConnectionLimit(1));

**Note:** Choosing a value of 1 for `hostConnectionLimit` eliminates contention for a host.

[Top](#Top)

<a name="Handling Synchronous Responses"></a>

#### Handling Synchronous Responses

In the simplest case, you can wait for a response, potentially forever. Typically, you should handle both a valid response and an exception:

        HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/");

        try {
          HttpResponse response1 = httpClient.apply(request).apply();
        } catch (Exception e) {
            ...
        }

[Top](#Top)

<a name="Handling Synchronous Responses With Timeouts"></a>

#### Handling Synchronous Responses With Timeouts

To avoid waiting forever for a response, you can specify a duration, which throws an exception if the duration expires. The following example sets a duration of 1 second:

        try {
          HttpResponse response2 = httpClient.apply(request).apply(
            new Duration(TimeUnit.SECONDS.toNanos(1)));
        } catch (Exception e) {
            ...
        }

[Top](#Top)

<a name="Handling Synchronous Responses With Exception Handling"></a>

#### Handling Synchronous Responses With Exception Handling

Use the `Try` and `Throw` classes in `com.twitter.util` to implement a more general approach to exception handling for synchronous responses. In addition to specifying a timeout duration, which can throw an exception, other exceptions can also be thrown.

        Try<HttpResponse> responseTry = httpClient.apply(request).get(
          new Duration(TimeUnit.SECONDS.toNanos(1)));
        if (responseTry.isReturn()) {
          // Cool, I have a response! Get it and do something
          HttpResponse response3 = responseTry.get();
          ...
        } else {
	        // Throw an exception
          Throwable throwable = ((Throw)responseTry).e();
          System.out.println("Exception thrown by client: " +  throwable);
        }

**Note:** You must call the request's `get` method instead of the `apply` method to retrieve the `Try` object.

[Top](#Top)

<a name="Handling Asynchronous Responses"></a>

#### Handling Asynchronous Responses

To handle asynchronous responses, you add a `FutureEventListener` to listen for a response. Finagle invokes the `onSuccess` method when a response arrives or invokes `onFailure` for an exception: 

        httpClient.apply(request).addEventListener(new FutureEventListener<HttpResponse>() {
          @Override
          public void onSuccess(HttpResponse response4) {
            // Cool, I have a response, do something with it!
            ...
          }

          @Override
          public void onFailure(Throwable throwable) {
            System.out.println("Exception thrown by client: " +  throwable);
          }
        });
      }
    }

[Top](#Top)

<a name="Implementing a Pool for Blocking Operations in Java"></a>

### Implementing a Thread Pool for Blocking Operations in Java

To prevent blocking operations from executing on the main Finagle thread, you must wrap the blocking operation in a Scala closure and execute the closure on the Java thread that you create. Typically, your Java thread is part of a thread pool. The following sections show how to wrap your blocking operation, set up a thread pool, and execute the blocking operation on a thread in your pool:

* <a href="#Wrapping the Blocking Operation">Wrapping the Blocking Operation</a>
* <a href="#Setting Up Your Thread Pool">Setting Up Your Thread Pool</a>
* <a href="#Invoking the Blocking Operation">Invoking the Blocking Operation</a>

**Note:** Jakob Homan provides an example implementation of a thread pool that executes Scala closures on <a href="https://github.com/jghoman/finagle-java-example">GitHub</a>. 

[Top](#Top)

<a name="Wrapping the Blocking Operation"></a>

#### Wrapping the Blocking Operation

The `Util` project contains a `Function0` class that represents a Scala closure. You can override the `apply` method to wrap your blocking operation:

    public static class BlockingOperation extends com.twitter.util.Function0<Integer> {
      public Integer apply() {
        // Implement your blocking operation here
        ...
      }
    }

[Top](#Top)

<a name="Setting Up Your Thread Pool"></a>

#### Setting Up Your Thread Pool

The following example shows a Thrift server that places the blocking operation defined in the `Future0` object's `apply` method in the Java thread pool, where it will eventually execute and return a result:

    public static class HelloServer implements Hello.ServiceIface {
      ExecutorService pool = Executors.newFixedThreadPool(4);                        // Java thread pool
      ExecutorServiceFuturePool futurePool = new ExecutorServiceFuturePool(threads); // Java Future thread pool
		
      public Future<Integer> blockingOperation() {  
	      Function0<Integer> blockingWork = new BlockingOperation();
        return futurePool.apply(blockingWork);
      }

      public static void main(String[] args) {
        Hello.ServiceIface processor = new Hello.ServiceIface();

        ServerBuilder.safeBuild(
          new Hello.Service(processor, new TBinaryProtocol.Factory()),
          ServerBuilder.get()
                       .name("HelloService")
                       .codec(ThriftServerFramedCodec.get())
                       .bindTo(new InetSocketAddress(8080))
          );
        )
      )
    )

[Top](#Top)

<a name="Invoking the Blocking Operation"></a>

#### Invoking the Blocking Operation

To invoke the blocking operation, you call the method that wraps your blocking operation and add an event listener that waits for either success or failure:

		  Service<ThriftClientRequest, byte[]> client = ClientBuilder.safeBuild(ClientBuilder.get()
		    .hosts(new InetSocketAddress(8080))
		    .codec(new ThriftClientFramedCodecFactory())
		    .hostConnectionLimit(100)); // Must be more than 1 to enable parallel execution

		  Hello.ServiceIface client =
		    new Hello.ServiceToClient(client, new TBinaryProtocol.Factory());

		  client.blockingOperation().addEventListener(new FutureEventListener<Integer>() {
		    public void onSuccess(Integer i) {
		      System.out.println(i);
		    }

		  public void onFailure(Throwable t) {
		    System.out.println("Exception! ", t.toString());
		  });

[Top](#Top)

<a name="Additional Samples"></a>

## Additional Samples

* [Echo](https://github.com/twitter/finagle/tree/master/finagle-example/src/main/scala/com/twitter/finagle/example/echo) - A simple echo client and server using a newline-delimited protocol. Illustrates the basics of asynchronous control-flow.
* [Http](https://github.com/twitter/finagle/tree/master/finagle-example/src/main/scala/com/twitter/finagle/example/http) - An advanced HTTP client and server that illustrates the use of Filters to compositionally organize your code. Filters are used here to isolate authentication and error handling concerns.
* [Memcached Proxy](https://github.com/twitter/finagle/tree/master/finagle-example/src/main/scala/com/twitter/finagle/example/memcachedproxy) - A simple proxy supporting the Memcached protocol.
* [Stream](https://github.com/twitter/finagle/tree/master/finagle-example/src/main/scala/com/twitter/finagle/example/stream) - An illustration of Channels, the abstraction for Streaming protocols.
* [Spritzer 2 Kestrel](https://github.com/twitter/finagle/tree/master/finagle-example/src/main/scala/com/twitter/finagle/example/spritzer2kestrel) - An illustration of Channels, the abstraction for Streaming protocols. Here the Twitter Firehose is "piped" into a Kestrel message queue, illustrating some of the compositionality of Channels.
* [Stress](https://github.com/twitter/finagle/tree/master/finagle-example/src/main/scala/com/twitter/finagle/example/stress) - A high-throughput HTTP client for driving stressful traffic to an HTTP server. Illustrates more advanced asynchronous control-flow.
* [Thrift](https://github.com/twitter/finagle/tree/master/finagle-example/src/main/scala/com/twitter/finagle/example/thrift) - A simple client and server for a Thrift protocol.
* [Kestrel Client](https://github.com/twitter/finagle/tree/master/finagle-example/src/main/scala/com/twitter/finagle/example/kestrel) - A client for doing reliable reads from one or more Kestrel servers.

[Top](#Top)

<a name="API Reference Documentation"></a>

## API Reference Documentation

* [Builders](http://twitter.github.com/finagle/api/finagle-core/com/twitter/finagle/builder/package.html)
* [Service](http://twitter.github.com/finagle/api/finagle-core/com/twitter/finagle/Service.html)
* [Future](http://twitter.github.com/util/util-core/target/site/doc/main/api/com/twitter/util/Future.html)
* [Complete Core Project Scaladoc](http://twitter.github.com/finagle/api/finagle-core/)
* [Kestrel](http://twitter.github.com/finagle/api/finagle-kestrel/index.html)
* [Memcached](http://twitter.github.com/finagle/api/finagle-memcached/index.html)
* [Ostrich 4](http://twitter.github.com/finagle/api/finagle-ostrich4/index.html)
* [ServerSets](http://twitter.github.com/finagle/api/finagle-serversets/index.html)
* [Stream](http://twitter.github.com/finagle/api/finagle-stream/index.html)
* [Thrift](http://twitter.github.com/finagle/api/finagle-thrift/index.html)
* [Complete Util Project Scaladoc](http://twitter.github.com/util/util-core/target/site/doc/main/api/)

For the software revision history, see the [Finagle change log](https://github.com/twitter/finagle/blob/master/ChangeLog).
For additional information about Finagle, see the [Finagle homepage](http://twitter.github.com/finagle/).

[Top](#Top)

# Administrivia

We use [Semantic Versioning](http://semver.org/) for published artifacts.
