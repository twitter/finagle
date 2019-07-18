All streaming metrics are automatically collected by Finagle by setting `isChunked` as true on Http
request or response. All metrics are traced using the binary annotations with a `clnt/<clnt_name>/`
prefix on the client side, and `srv/` prefix on the server side.

The following metrics are added by
:finagle-http-src:`StreamingStatsFilter <com/twitter/finagle/http/filter/StreamingStatsFilter.scala>`

**http/stream/request/closed**
  A counter of the number of closed request streams.

**http/stream/request/duration_ms**
  A histogram of the duration of the lifetime of request streams, from the time a stream is
  initialized until it's closed, in milliseconds.

**http/stream/request/failures**
  A counter of the number of times any failure has been observed in the middle of a request stream.

**http/stream/request/failures/<exception_name>**
  A counter of the number of times a specific exception has been thrown in the middle of a request
  stream.

**http/stream/request/opened**
  A counter of the number of opened request streams.

**http/stream/request/pending**
  A gauge of the number of pending request streams.

**http/stream/response/closed**
  A counter of the number of closed response streams.

**http/stream/response/duration_ms**
  A histogram of the duration of the lifetime of response streams, from the time a stream is
  initialized until it's closed, in milliseconds.

**http/stream/response/failures**
  A counter of the number of times any failure has been observed in the middle of a response stream.

**http/stream/response/failures/<exception_name>**
  A counter of the number of times a specific exception has been thrown in the middle of a response
  stream.

**http/stream/response/opened**
  A counter of the number of opened response streams.

**http/stream/response/pending**
  A gauge of the number of pending response streams.

**stream/request/chunk_payload_bytes** `verbosity:debug`
  A histogram of the number of bytes per chunk's payload of request streams. This is measured in
  :finagle-http-src:`c.t.finagle.http.filter.PayloadSizeFilter <com/twitter/finagle/http/filter/PayloadSizeFilter.scala>`

**stream/response/chunk_payload_bytes** `verbosity:debug`
  A histogram of the number of bytes per chunk's payload of response streams. This is measured in
  :finagle-http-src:`c.t.finagle.http.filter.PayloadSizeFilter <com/twitter/finagle/http/filter/PayloadSizeFilter.scala>`

You could derive the streaming success rate of:
  - the total number of streams
    number of successful streams divided by number of total streams
  - closed streams
    number of successful streams divided by number of closed streams
Here we assume a success stream as a stream terminated without an exception or a stream that has not
terminated yet.

Take request stream as an example, assuming your counters are not "latched", which means that their
values are monotonically increasing:

  # Success rate of total number of streams:
  1 - (rated_counter(stream/request/failures)
       / (gauge(stream/request/pending) + rated_counter(stream/request/closed)))

  # Success rate of number of closed streams:
  1 - (rated_counter(stream/request/failures) / rated_counter(stream/request/closed))
