.. _metrics_stats_filter:

StatsFilter
<<<<<<<<<<<

**requests**
  A counter of the total number of successes + failures.

**success**
  A counter of the total number of successes.

**request_latency_ms**
  A histogram of the latency of requests in milliseconds.

**pending**
  A gauge of the current total number of outstanding requests.

**failures/<exception_name>+**
  A counter of the number of times a specific exception has been thrown.
  If you are using a ``ResponseClassifier`` that classifies non-Exceptions
  as failures, it will use a synthetic Exception,
  ``com.twitter.finagle.service.ResponseClassificationSyntheticException``,
  to account for these.

**failures**
  A counter of the number of times any failure has been observed.

**sourcedfailures/<source_service_name>{/<exception_name>}+**
  A counter of the number of times a specific
  :src:`SourcedException <com/twitter/finagle/Exceptions.scala>` or sourced
  :src:`Failure <com/twitter/finagle/Failure.scala>` has been thrown. Sourced
  failures include additional information on what service caused the failure.

**sourcedfailures/<source_service_name>**
  A counter of the number of times any
  :src:`SourcedException <com/twitter/finagle/Exceptions.scala>` or sourced
  :src:`Failure <com/twitter/finagle/Failure.scala>` has been thrown from this
  service. Sourced failures include additional information on what service
  caused the failure.

**sourcedfailures**
  A counter of the number of times any
  :src:`SourcedException <com/twitter/finagle/Exceptions.scala>` or sourced
  :src:`Failure <com/twitter/finagle/Failure.scala>` has been thrown. Sourced
  failures include additional information on what service caused the failure.

StatsFactoryWrapper
<<<<<<<<<<<<<<<<<<<

.. _service_factory_failures:

**failures/<exception_class_name>**
  A counter of the number of times :doc:`Service <ServicesAndFilters>`
  creation has failed with this specific exception.

**failures**
  A counter of the number of times :doc:`Service <ServicesAndFilters>`
  creation has failed.

**service_acquisition_latency_ms**
  A stat of the latency, in milliseconds, to acquire a service.
  This entails establishing a connection or waiting for a connection from a pool.

ServerStatsFilter
<<<<<<<<<<<<<<<<<

**handletime_us**
  A  stat of the time it takes to handle the request in microseconds.
  This is how long it takes to set up the chain of ``Future``\s to be used in the
  response without waiting for the response. Large values suggest blocking code
  on a Finagle thread.

**transit_latency_ms**
  A stat that attempts to measure (wall time) transit times between hops, e.g.,
  from client to server. Be aware that clock drift between hosts and other factors
  can contribute here. Not supported by all protocols.

**deadline_budget_ms**
  A stat accounting for the (implied) amount of time remaining for this request,
  for example from a deadline or timeout. Not supported by all protocols.

RequestSemaphoreFilter
<<<<<<<<<<<<<<<<<<<<<<

.. _requests_concurrency_limit:

**request_concurrency**
  A gauge of the total number of current concurrent requests.

**request_queue_size**
  A gauge of the total number of requests which are waiting because of the limit
  on simultaneous requests.

PayloadSizeFilter (enabled for Mux, HTTP (non-chunked), Thrift)
<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

**request_payload_bytes**
  A histogram of the number of bytes per request's payload.

**response_payload_bytes**
  A histogram of the number of bytes per response's payload.

TimeoutFilter
<<<<<<<<<<<<<

**timeout/expired_deadline_ms**
  A stat of the elapsed time since expiry if a deadline has expired, in
  milliseconds. Temporary stat to aid in debugging.