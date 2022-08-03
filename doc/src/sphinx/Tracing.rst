.. _finagle_tracing

Tracing
=======

Finagle generates distributed trace data for each request. Traces allow one to understand and debug how distributed systems work. Traces don't just capture timing information, they are able to hold any number of different types of information. Information such as payload sizes, serialization/deserialization times, as well as information about retries and backup requests can be logged for each request. This data is collected as a series of causally related segments, or spans, and is sent off for storage at each hop of a request's path.

Finagle tracing represents pieces of a request/response path as spans. Each span shares a single 64-bit traceid which allows them to later be aggregated and analyzed as a complete trace. Likewise span ids are shared between hops, between a client and a server, in a trace. Systems such as `Zipkin <http://zipkin.io>`_, which we use at Twitter, can be used to collect, correlate, and view this data.

Configuration
-------------

By default Finagle uses a `DefaultTracer` which will load implementations from included artifacts using the Java `ServiceLoader <https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`_. Out of the box, adding the finagle-zipkin-scribe package to your classpath will enable sending tracing spans to zipkin via the `scribe <http://go/scribe>`_ protocol.

You can replicate this functionality and globally install a tracer by including an artifact with a service loaded `Tracer`. Your implementation must implement the `Tracer <https://github.com/twitter/finagle/blob/develop/finagle-core/src/main/scala/com/twitter/finagle/tracing/Tracer.scala>`_ API and follow the requirements necessary to be loaded by the `ServiceLoader <https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html>`_.

Furthermore, tracers can be overridden in code, they may be specified when creating a client or server. In the following example we replace the default tracer with our own `BroadcastTracer` which will send traces both to zipkin, via scribe, as well as to the console:

.. code-block:: scala

  import com.twitter.finagle.Http
  import com.twitter.finagle.tracing.{BroadcastTracer, Record, TraceId, Tracer}
  import com.twitter.finagle.zipkin.thrift.ZipkinTracer

  object SystemOutTracer extends Tracer {
    def record(record: Record): Unit = println(record)
    def sampleTrace(traceId: TraceId): Option[Boolean] = Some(false)
  }

  val client = Http.client
    .withTracer(BroadcastTracer(Seq(
      ZipkinTracer.mk(),
      SystemOutTracer
    )))

We also supply a `NullTracer` which will not write trace messages anywhere.  However, even if `NullTracer` is passed to `withTracer`, Finagle clients will continue to propagate trace information.  This can have the surprising side effect of orphaning traces if the traces are being aggregated in a distributed tracing service like Zipkin, so we generally discourage people from using `NullTracer` in production.

Information about active traces is located within the Finagle `Context <Context>`_. The Trace API is a convenient way of accessing the currently active traces for a request and provides the ability to manipulate them with operations such as adding custom annotations.

.. code-block:: scala

  import com.twitter.finagle.http.{Request, Response}
  import com.twitter.finagle.tracing.Trace
  import com.twitter.finagle.{Service, SimpleFilter}
  import com.twitter.util.Future

  class MyFilter extends SimpleFilter[Request, Response] {
    def apply(request: Request, service: Service[Request, Response]): Future[Response] = {
      val header = request.headerMap("Special-Header")
      Trace.recordBinary("special-header", header)
      service(request)
    }
  }

Available Tracers
-----------------

NullTracer
~~~~~~~~~~

A tracer that drops all traces on the ground. This is the default behavior if no tracer is configured.

BroadcastTracer
~~~~~~~~~~~~~~~

A tracer that will forward all traces to any number of child tracers. This is useful if you want spans sent to multiple backend systems. By default all service loaded tracers are bundled under a single `BroadcastTracer`.

SamplingTracer
~~~~~~~~~~~~~~

A tracer that will only forward a subset of traces to the backing store. This is useful when a service has a high request rate and you do not want to overwhelm the tracing system. Sampling is determined when a new trace is created. If the trace survives sampling this decision will be recorded and passed along with the trace so that all spans along the way will also survive sampling.

RawZipkinTracer
~~~~~~~~~~~~~~~

An abstract `SamplingTracer` which formats traces for consumption by Zipkin but leaves the transport open to implementation.

ScribeZipkinTracer
~~~~~~~~~~~~~~~~~~

A `RawZipkinTracer` that sends spans to Zipkin using the Scribe protocol. This tracer is included with the finagle-zipkin-scribe artifact and exposes the following flags for configuration:

-com.twitter.finagle.zipkin.host
-com.twitter.finagle.zipkin.initialSampleRate

Annotations
-----------

Annotations are tied directly to a specific span and note what was going on at the time that span was active. In Finagle, the current span and it's annotations are kept in the `Context <Context>`_. There are primarily two types of annotations that can be used throughout a trace: annotations, and binary annotations. the main difference between the two types is that plain annotations describe a point in time event and carry a timestamp, while binary annotations only contain information about a span, a simple mapping from a string key to a binary value, and do not contain a timestamp. For example, a "Client Send" event occurs at a specific time and is accompanied by a timestamp so that it can be sequenced and compared with a "Wire Send" event. This information can be used to understand how long it takes for a request to be handed from the client to the network card for transit to a remote server. By comparison, a binary annotation might contain information with regards to what was happening with the local system at the time of the request, for instance, information about what the garbage collector (GC) was doing at the time of the request.

Trace System Initialization
---------------------------

The tracing system is initialized by the `TraceInitializationFilter`. This filter is present in the default Finagle stack for both the client and the server. The role of this filter is to set up the tracing subsystem and wrap incoming/outgoing requests with the correct tracing context, this includes either using the incoming trace id or generating a new one, the effect being a fully enabled tracing system in the current context.

Tracing
-------

In general you can use any method from the `Trace` API to add annotations to the current tracing context. This allows you to annotate events or information happening at the time a request is being made or processed.

Each request operates within the scope of a span, which is created when the request is received or an initial request is made. In some cases it may be advantageous to segment parts of processing a request into their own discrete events. You can do this by generating local spans; these spans exist completely within a single process. This is useful when understanding the relationship between local computation and external requests as well as relative timing and duration of these operations to each other. Local spans can be created with the `Trace#traceLocal` methods.

.. code-block:: scala

  import com.twitter.finagle.tracing.Trace

  def chainOfEvents(): Int = ???

  Trace.traceLocal("important_work") {
    // perform within the context of a new SpanId
    chainOfEvents()
  }

Furthermore, operations can be timed and the result recorded within the trace context:

.. code-block:: scala

  import com.twitter.finagle.tracing.Trace

  def complexComputation(): Int = ???

  val result = Trace.time("complexComputation_ns") {
    // record how long the computation took
    complexComputation()
  }

Combining a local span with timing information allows for comparing the performance of a local computation to other distributed computations.

Standard Annotations
--------------------

=======================  ==================================================  ==============================
Annotation               Description                                         Location Traced
=======================  ==================================================  ==============================
Wire Send                Time a message is handed to the transport layer     `WireTracingFilter`
Wire Receive             Time a message is received by the transport layer   `WireTracingFilter`
Wire Receive Error       Any error message generated by the transport layer  `WireTracingFilter`
Client Send              Time a request is sent by a client                  `ClientTracingFilter`
Client Receive           Time a response is received by a client             `ClientTracingFilter`
Client Receive Error     Any error message generated by the client stack     `ClientTracingFilter`
Server Send              Time a request is received by a server              `ServerTracingFilter`
Server Receive           Time a response is sent by a server                 `ServerTracingFilter`
Server Send Error        Any error generated by the server stack             `ServerTracingFilter`
Service Name             The service name where this span originated         `TraceInitializationFilter`
RPC                      The RPC name and method                             service/client should record
Message                  Informational message                               `RetryFilter`, `TimeoutFilter`
Client Address           Originating address for a request                   `ClientDispatcher`
Server Address           Terminal address for an address                     `DestinationTracing`
Local Address            Address where a span is generated                   `DestinationTracing`
=======================  ==================================================  ==============================

Additional Binary Annotations
-----------------------------

Backup Requests
~~~~~~~~~~~~~~~

srv/backup_request_processing (`BackupRequest`)
```````````````````````````````````````````````
A binary annotation generated on a server when processing a backup request.

clnt/backup_request_threshold_ms (`BackupRequestFilter`)
````````````````````````````````````````````````````````
The configured backup request threshold in milliseconds.

clnt/backup_request_span_id (`BackupRequestFilter`)
```````````````````````````````````````````````````
The span id for the backup request, this is useful to be able to analyze which spans are responsible for the main request and which spans are responsible for the backup request.

Client Backup Request Issued (`BackupRequestFilter`)
````````````````````````````````````````````````````
A Message annotation generated by the `BackupRequestFilter` when a backup request is issued.

Client Backup Request Won (`BackupRequestFilter`)
`````````````````````````````````````````````````
A Message type annotation generated by the `BackupRequestFilter` when a backup request has completed before the main request.

Client Backup Request Lost (`BackupRequestFilter`)
``````````````````````````````````````````````````
A Message type annotation generated by the `BackupRequestFilter` when a backup request completed after the main request.

Deadlines
~~~~~~~~~
Note that these annotations can be traced on the client side as well, but since Finagle handles deadlines only on the server side by default, we list only `srv/`-prefixed annotations here.  

srv/request_deadline
````````````````````
The time at which the deadline will be reached.

srv/request_deadline_remaining_ms
`````````````````````````````````
The time remaining, in milliseconds, before the deadline is reached. This appears only when the deadline has not yet been reached.

srv/request_deadline_exceeded_ms
````````````````````````````````
The time elapsed, in milliseconds, beyond the deadline. This appears only when the deadline has been exceeded.

srv/request_deadline_rejected
`````````````````````````````
A boolean value representing whether a request has been rejected due to exceeding the deadline. It appears only when a request has been rejected.

Garbage Collection
~~~~~~~~~~~~~~~~~~

GC Start (`MkJvmFilter`)
````````````````````````
A Message type annotation generated for the current trace with the beginning timestamp of all gc events within the last minute before the request.

GC End (`MkJvmFilter`)
``````````````````````
A Message type annotation generated for the current trace with the timestamp of the end of all gc events within the last minute before the request.

jvm/gc_count (`MkJvmFilter`)
````````````````````````````
The number of gc cycles in the last minute before the request.

jvm/gc_ms (`MkJvmFilter`)
`````````````````````````
The total amount of time spent on gc within the last minute before the request.

Payload Size
~~~~~~~~~~~~

clnt/request_payload_bytes (`PayloadSizeFilter`)
````````````````````````````````````````````````
The size of the payload for a request sent by a client.

srv/request_payload_bytes (`PayloadSizeFilter`)
```````````````````````````````````````````````
The size of the payload for a request received by a server.

clnt/response_payload_bytes (`PayloadSizeFilter`)
`````````````````````````````````````````````````
The size of the payload for a response received by a client.

srv/response_payload_bytes (`PayloadSizeFilter`)
````````````````````````````````````````````````
The size of the payload for a response sent by a server.

Request/Response Serialization
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

clnt/request_serialization_ns (`ClientTraceAnnotationFilter`)
`````````````````````````````````````````````````````````````
The total amount of time taken by a client to serialize a request.

clnt/response_deserialization_ns (`ClientTraceAnnotationFilter`)
````````````````````````````````````````````````````````````````
The total amount of time taken by a client to deserialize a response.

srv/request_deserialization_ns (Generated by Scrooge)
`````````````````````````````````````````````````````
The total amount of time taken by a server to deserialize a request.

srv/response_serialization_ns (Generated by Scrooge)
````````````````````````````````````````````````````
The total amount of time taken by a server to serialize a response.

Retry
~~~~~

finagle.retry (`RetryFilter`)
`````````````````````````````
Record when a retry has occurred

Samping
~~~~~~~

zipkin.sampling_rate (`SamplingTracer`)
``````````````````````````````````````
Record the sampling rate at trace roots

Timeout
~~~~~~~

finagle.timeout (`TimeoutFilter`)
`````````````````````````````````
Record when a timeout occurs

Offload
~~~~~~~

clnt/OffloadFilter: Offloaded continuation from IO threads to pool with ${num} workers (`OffloadFilter`)
````````````````````````````````````````````````````````````````````````````````````````````````````````
When a request is offloaded by a client to a different pool, contains information about the size of the pool.

srv/OffloadFilter: Offloaded continuation from IO threads to pool with ${size} workers (`OffloadFilter`)
````````````````````````````````````````````````````````````````````````````````````````````````````````
When a request is offloaded by a server to a different pool, contains information about the size of the pool.

DarkTraffic
~~~~~~~~~~~

clnt/dark_request (`DarkTrafficFilter`)
```````````````````````````````````````
A binary annotation which signifies that the span is a dark request.

clnt/dark_request_key (`DarkTrafficFilter`)
```````````````````````````````````````````
A binary annotation contained in the light and the dark request with the same id so the two requests can be identified as related.
