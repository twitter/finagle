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
  a counter of the number of times a specific SourcedException has been thrown

**sourcedfailures/<source_service_name>**
  a counter of the number of times any SourcedException has been thrown from this service

**sourcedfailures**
  a counter of the number of times any SourcedException has been thrown

StatsFactoryWrapper
<<<<<<<<<<<<<<<<<<<

**failures/<exception_class_name>**
  a counter of the number of times a service factory has failed with this specific exception

**failures**
  a counter of the number of times a service factory has failed

HandletimeFilter
<<<<<<<<<<<<<<<<

**handletime_us**
  a histogram of the time it takes to handle the request in microseconds.
  NB: does not include the time to respond

DefaultServer
<<<<<<<<<<<<<

**request_concurrency**
  a gauge of the total number of current concurrent requests

**request_queue_size**
  a gauge of the total number of requests which are waiting because of the limit
  on simultaneous requests
