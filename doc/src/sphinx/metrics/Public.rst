StatsFilter
<<<<<<<<<<<

**requests**
  a counter of the total number of successes + failures

**success**
  a counter of the total number of successes

**request_latency_ms**
  a histogram of the latency of requests in milliseconds

**load**
  a gauge of the current total number of outstanding requests

**pending**
  a gauge of the current total number of outstanding requests

**failures/<exception_name>+**
  a counter of the number of times a specific exception has been thrown

**failures**
  a counter of the number of times any exception has been thrown

**sourcedfailures/<source_service_name>{/<exception_name>}+**
  a counter of the number of times a specific SourcedException or sourced
  Failure has been thrown

**sourcedfailures/<source_service_name>**
  a counter of the number of times any SourcedException or sourced Failure has
  been thrown from this service

**sourcedfailures**
  a counter of the number of times any SourcedException or sourced Failure has
  been thrown

StatsFactoryWrapper
<<<<<<<<<<<<<<<<<<<

**failures/<exception_class_name>**
  a counter of the number of times a service factory has failed with this
  specific exception

**failures**
  a counter of the number of times a service factory has failed

**service_acquisition_latency_ms**
  a stat of the latency to acquire a service in milliseconds
  this entails establishing a connection or waiting for a connection from a pool

ServerStatsFilter
<<<<<<<<<<<<<<<<<

**handletime_us**
  a histogram of the time it takes to handle the request in microseconds
  NB: does not include the time to respond

**transit_latency_ms**
  a stat that attempts to measure (walltime) transit times between hops, e.g.,
  from client to server. Not supported by all protocols.

**deadline_budget_ms**
  a stat accounting for the (implied) amount of time remaining for this request,
  for example from a deadline or timeout. Not supported by all protocols.

DefaultServer
<<<<<<<<<<<<<

**request_concurrency**
  a gauge of the total number of current concurrent requests

**request_queue_size**
  a gauge of the total number of requests which are waiting because of the limit
  on simultaneous requests
