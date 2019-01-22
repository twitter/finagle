# OpenCensus Tracing

This module integrates Finagle with [OpenCensus](https://opencensus.io/) tracing.

## Current State

This library is in an experimental state.

## Details

This lets Finagle [clients](https://twitter.github.io/finagle/guide/Clients.html)
and [servers](https://twitter.github.io/finagle/guide/Servers.html) participate
in OpenCensus tracing as requests flow from a Finagle client to a Finagle server
to an OpenCensus-instrumented service (e.g.
[GCP's Bigtable](https://cloud.google.com/bigtable/)).

Clients and servers can install this functionality by using `StackClientOps` and
`StackServerOps`.

Notes:

 - This is not an implementation of Finagle's tracing APIs (`c.t.f.tracing.Tracer`)
 - HTTP and ThriftMux are the supported protocols

### Exporters

This module does not specify an OpenCensus
[exporter](https://opencensus.io/core-concepts/exporters/). As such, your
application must add the proper dependencies to export to a specific backend.

For example, to use [GCP Stackdriver](https://cloud.google.com/stackdriver/)
you would add the dependency on `io.opencensus:opencensus-exporter-trace-stackdriver`.
