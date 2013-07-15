# finagle-zipkin

*finagle-zipkin* provides the instrumentation for Dapper-style
distributed tracing in Finagle-based services. See
<a href="https://github.com/twitter/zipkin">Zipkin</a> for the
collection and query services.

## Terminology
* Annotation - includes a value, timestamp, and host
* Span - a set of annotations that correspond to a particular RPC
* Trace - a set of spans that share a single root span

We define four Core Annotations that fully describe an RPC (client send,
server receive, server send, client receive)

## Architecture
The `ZipkinTracer` is a Finagle `Tracer` that collects trace data.

### Instrumentation

### Sampling
The default configuration samples 1/1000 traces, computed as a hash of
the trace id.

### In memory structures
`ZipkinTracer` keeps some in memory data structures that buffer Spans
as they are mutated to prevent making too many I/O calls.

### Data sink
`ZipkinTracer` sends data to a local Scribe daemon. This allows tracing
to be as low overhead as possible since Scribe can choose to drop data
when under heavy load.

## Setup
To add tracing to your Finagle service, just add one line to the
`ServerBuilder` or `ClientBuilder` instantiation

```
  ServerBuilder()
    .codec(ThriftServerFramedCodec())
    .bindTo(serverAddr)
    .name("servicename")
    .tracer(ZipkinTracer())
    .build()
```

### Configuration
By default, `ZipkinTracer` sends trace data to a local Scribe daemon.
This can be configured to send the data either directly to a Zipkin
Collector or some other service. In addition, the sample rate can also
be configured to a floating point value between 0.0 and 1.0.

```
  ZipkinTracer(scribeHost = "10.0.0.1", scribePort = 1234, sampleRate = 0.4)
```

### Custom annotations
Additional information can be added in application code with custom
annotations. These can be either time-based, or key-value based.

```
  Trace.record("starting some extremely expensive computation")
  Trace.recordBinary("http.response.code", "500")
```

