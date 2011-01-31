# Finagle

## What is it?

Finagle is an async network client and server built on top of [Netty](http://www.jboss.org/netty). There are presently codecs for the [HTTP](http://www.w3.org/Protocols/rfc2616/rfc2616.html) and [Thrift](http://thrift.apache.org/) protocols, though it is trivial to add others.

## Where do I start?

Here are some quick examples of asynchronous an HTTP client and server:

* Scala:

        src/main/scala/com/twitter/finagle/test/Http{Client,Server}.scala

* Java:

        src/main/java/com/twitter/finagle/javaapi/Http{Client,Server}Test.java

### Writing clients

Finagle includes [ClientBuilder](http://twitter.github.com/finagle/com/twitter/finagle/builder/ClientBuilder.html) for the convenient construction of clients.

    val client =
      ClientBuilder()
        .name("http")
        .codec(Http)
        .hosts("localhost:10000,localhost:10001,localhost:10003")
        .retries(2) // per-request retries (for timeouts, connection failures)
        .exportLoadsToOstrich() // export host-level load data to ostrich
        .logger(Logger.getLogger("http"))
        .build()

This creates a load balanced HTTP client that balances requests among 3 local endpoints. This client balances load using [LoadBalancedBroker](http://twitter.github.com/finagle/com/twitter/finagle/channel/LoadBalancedBroker.html). If retries are specified (using retries(Int)), those are achieved through the [RetryingBroker](http://twitter.github.com/finagle/com/twitter/finagle/channel/RetryingBroker.html) class, and if initialBackoff and backoffMultiplifer are specified, by [ExponentialBackoffRetryingBroker](http://twitter.github.com/finagle/com/twitter/finagle/channel/ExponentialBackoffRetryingBroker.html).

After construction of the client, make some requests with it:

      val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1,
                                           HttpMethod.GET,
                                           "/")
      client(request).addListener(new FutureEventListener[HttpResponse] {
        def onSuccess(response: HttpResponse) {
          println("Success! %s".format(response))
        }

        def onFailure(cause: Throwable) {
          println("Shame! %s".format(cause.getMessage))
        }
      })

### Writing servers

There is also a builder for servers.

You can write services in terms of the [Service](http://twitter.github.com/finagle/com/twitter/finagle/service/Service.html) class, or if you need more control, wire up a [custom pipeline handlers](http://docs.jboss.org/netty/3.2/api/org/jboss/netty/channel/ChannelPipelineFactory.html).

Here's a service class that responds to any HTTP call with "yo" (very useful):

    val service = new Service[HttpRequest, HttpResponse] {
      def apply(request: HttpRequest) = Future {
        val response = new DefaultHttpResponse(
          HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
        response.setContent(ChannelBuffers.wrappedBuffer("yo".getBytes))
        response
      }
    }

This class takes HttpRequest objects and returns Future[HttpResponse].

We can start up the server and bind it to a port like this:

    ServerBuilder()
     .codec(Http)                           // Speak HTTP
     .reportTo(Ostrich())                   // Report the stats to Ostrich
     .service(service)                      // Process requests using service
     .bindTo(new InetSocketAddress(10000))  // Bind port 10000 on all interfaces
     .build

If you want to write your service in terms of a Netty ChannelPipelineFactory, pass it in like so:

    val pf = new ChannelPipelineFactory() {
      def getPipeline = {
        val pipeline = Channels.getPipeline
        pipeline.addLast("handler", new SimpleChannelUpstreamHandler { ... })
        pipeline
      }
    }

    ServerBuilder()
     .codec(Http)                           // Speak HTTP
     .reportTo(Ostrich())                   // Report the stats to Ostrich
     .pipelineFactory(pf)                   // Process requests on this pipeline
     .bindTo(new InetSocketAddress(10000))  // Bind port 10000 on all interfaces
     .build


Here is the (poorly) documented API: [ServerBuilder](http://twitter.github.com/finagle/com/twitter/finagle/builder/ServerBuilder.html)



## API docs

Some incomplete API documentation is available:
See [scaladoc](http://twitter.github.com/finagle/).
