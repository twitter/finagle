These metrics are added by
:finagle-http-src:`StreamingStatsFilter <com/twitter/finagle/http/filter/StreamingStatsFilter.scala>`
and can be enabled by setting `isChunked` as true on Http request and response.

**stream/request/closed**
  A counter of the number of closed request streams.

**stream/request/duration_ms**
  A histogram of the duration of the lifetime of request streams, from the time a stream is
  initialized until it's closed, in milliseconds.

**stream/request/failures**
  A counter of the number of times any failure has been observed in the middle of a request stream.

**stream/request/failures/<exception_name>**
  A counter of the number of times a specific exception has been thrown in the middle of a request
  stream.

**stream/request/opened**
  A counter of the number of opened request streams.

**stream/request/pending**
  A gauge of the number of pending request streams.

**stream/response/closed**
  A counter of the number of closed response streams.

**stream/response/duration_ms**
  A histogram of the duration of the lifetime of response streams, from the time a stream is
  initialized until it's closed, in milliseconds.

**stream/response/failures**
  A counter of the number of times any failure has been observed in the middle of a response stream.

**stream/response/failures/<exception_name>**
  A counter of the number of times a specific exception has been thrown in the middle of a response
  stream.

**stream/response/opened**
  A counter of the number of opened response streams.

**stream/response/pending**
  A gauge of the number of pending response streams.

**stream/request/chunk_payload_bytes** `verbosity:debug`
  A histogram of the number of bytes per chunk's payload of request streams. This is measured in
  c.t.finagle.http.filter.PayloadSizeFilter.

**stream/response/chunk_payload_bytes** `verbosity:debug`
  A histogram of the number of bytes per chunk's payload of response streams. This is measured in
  c.t.finagle.http.filter.PayloadSizeFilter.

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
